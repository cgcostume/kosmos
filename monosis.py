#!/usr/bin/env python3
"""
Monosis - Ancient Greek μόνωσις (reduction to unity)

A high-performance file deduplication tool that reduces many scattered copies
to their essential singular form. Designed to handle hundreds of thousands of
files efficiently with multi-stage processing and intelligent caching.

Features:
- Location-based management (multiple sources, single target)
- Multi-stage hashing pipeline for performance
- Persistent caching between runs
- Safe consolidation with copy-only operations
- Zero data loss guarantee with explicit approval for deletions

Workflow:
1. locations - Configure source and target directories
2. scan      - Analyze configured locations for duplicates
3. analyze   - Show duplicate groups (within and across locations)
4. consolidate - Copy unique files to target location
5. clean-sources - Remove verified duplicates from sources (with approval)
"""

import argparse
import json
import os
import pathlib
import sqlite3
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

# Local imports
from console_ui import ConsoleUI
from duplicate_detector import DuplicateDetector
from file_operations import FileOperations, OperationType
from monosis_config import ConfigManager, MonosisConfig

# Fix Windows console encoding for Unicode characters
if sys.platform == "win32":
    import io

    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")


@dataclass
class DuplicateGroup:
    """Information about a group of duplicate files"""

    hash_value: str
    file_paths: list[pathlib.Path]
    file_size: int
    total_size: int
    wasted_space: int
    locations: set[str] = field(default_factory=set)  # Which source locations contain this file

    @property
    def count(self) -> int:
        return len(self.file_paths)

    @property
    def is_cross_location(self) -> bool:
        """True if duplicates exist across multiple locations"""
        return len(self.locations) > 1


@dataclass
class LocationStats:
    """Statistics for a source location"""

    path: pathlib.Path
    total_files: int = 0
    unique_files: int = 0
    duplicate_files: int = 0
    within_location_duplicates: int = 0
    cross_location_duplicates: int = 0
    total_size: int = 0
    wasted_space: int = 0


class Monosis:
    """Main application class for Monosis deduplication"""

    def __init__(self, args):
        self.args = args
        self.ui = ConsoleUI()
        self.duplicate_detector = DuplicateDetector(hash_algorithm="xxhash64")
        self.file_operations = FileOperations()

        # Cache configuration
        self.cache_dir = pathlib.Path.home() / ".monosis"
        self.cache_db = self.cache_dir / "cache.db"
        self.scan_results_file = self.cache_dir / "scan_results.json"

        # Ensure cache directory exists
        self.cache_dir.mkdir(exist_ok=True)

        # Initialize configuration manager
        self.config_manager = ConfigManager(self.cache_dir)
        self.config = self.config_manager.load()

        # Initialize cache database
        self._init_cache_db()

        # Load existing cache into DuplicateDetector
        self._load_cache_into_detector()

    def _init_cache_db(self):
        """Initialize SQLite cache database"""
        with sqlite3.connect(self.cache_db) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS file_hashes (
                    file_path TEXT PRIMARY KEY,
                    file_size INTEGER,
                    mtime REAL,
                    quick_hash TEXT,
                    full_hash TEXT,
                    last_scan REAL
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_full_hash ON file_hashes(full_hash)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_size ON file_hashes(file_size)
            """)

    def _load_cache_into_detector(self):
        """Load cached hashes into the duplicate detector for faster lookups"""
        if not self.cache_db.exists():
            return

        with sqlite3.connect(self.cache_db) as conn:
            cursor = conn.execute("""
                SELECT file_path, full_hash FROM file_hashes 
                WHERE full_hash IS NOT NULL
            """)

            for file_path, full_hash in cursor:
                # Load into detector's cache
                self.duplicate_detector._hash_cache[file_path] = full_hash

    def show_configuration(self):
        """Show current configuration"""
        config = {}

        # Always show locations
        config["Source locations"] = self.config.source_locations if self.config.source_locations else ["undefined"]
        config["Target location"] = self.config.target_location or "undefined"
        config["Reference location"] = self.config.reference_location or "undefined"

        if self.config.last_scan:
            config["Last scan"] = self.config.last_scan

        # Command-specific configuration
        if hasattr(self.args, "command"):
            config["Command"] = self.args.command

            if self.args.command == "scan":
                config["Recursive"] = "Yes" if self.args.recursive else "No"
                config["Use cache"] = "Yes" if not self.args.no_cache else "No"
                config["Quick scan"] = "Yes" if self.args.quick else "No"
                if self.args.extensions:
                    config["Extensions"] = ", ".join(self.args.extensions)

            elif self.args.command == "consolidate":
                config["Dry run"] = "Yes" if self.args.dry_run else "No"
                config["Interactive"] = "Yes" if self.args.interactive else "No"
                if hasattr(self.args, "strategy"):
                    config["Strategy"] = self.args.strategy

            elif self.args.command == "clean-sources":
                config["Dry run"] = "Yes" if self.args.dry_run else "No"
                config["Interactive"] = "Yes" if self.args.interactive else "No"

        self.ui.show_configuration(config)

    def cmd_locations(self):
        """Manage source and target locations"""
        if hasattr(self.args, "location_command"):
            if self.args.location_command == "add":
                return self._locations_add()
            if self.args.location_command == "remove":
                return self._locations_remove()
            if self.args.location_command == "target":
                return self._locations_set_target()
            if self.args.location_command == "reference":
                return self._locations_set_reference()
            if self.args.location_command == "clear":
                return self._locations_clear()

        # Default: list locations
        return self._locations_list()

    def _locations_add(self):
        """Add source locations"""
        added = 0
        for path in self.args.paths:
            # Handle network paths on Windows
            resolved_path = self._resolve_network_path(path)
            if not resolved_path:
                continue

            if not resolved_path.exists():
                self.ui.print_error(f"Path does not exist: {resolved_path}")
                continue
            if not resolved_path.is_dir():
                self.ui.print_error(f"Not a directory: {resolved_path}")
                continue

            # Safety check: source cannot contain the target location
            if self.config.target_location:
                source_resolved = str(path.resolve())
                target_path = pathlib.Path(self.config.target_location).resolve()

                # Check if target is inside this source
                if str(target_path).startswith(source_resolved + os.sep) or str(target_path) == source_resolved:
                    self.ui.print_error(f"Cannot add source that contains target location: {target_path}")
                    self.ui.print_error("This would create circular references and data loss risks.")
                    continue

                # Check if this source is inside target (also problematic)
                if source_resolved.startswith(str(target_path) + os.sep):
                    self.ui.print_error(f"Cannot add source inside target location: {target_path}")
                    self.ui.print_error("Choose a source location outside the target directory.")
                    continue

            if self.config.add_source(resolved_path):
                self.ui.print_success(f"Added source location: {resolved_path.resolve()}")
                added += 1
            else:
                self.ui.print_warning(f"Location already configured: {resolved_path.resolve()}")

        if added > 0:
            self.config_manager.save(self.config)

        return True

    def _resolve_network_path(self, path: pathlib.Path) -> Optional[pathlib.Path]:
        r"""Resolve network paths and handle Windows UNC paths like \\server\share"""
        try:
            path_str = str(path)

            # Handle Windows UNC paths (\\server\share)
            if sys.platform == "win32" and path_str.startswith("\\\\"):
                # Convert to WindowsPath with proper UNC handling
                return pathlib.WindowsPath(path_str)

            # Handle regular paths
            return path.resolve()

        except (OSError, ValueError) as e:
            self.ui.print_error(f"Invalid path format: {path} ({e})")
            return None

    def _locations_remove(self):
        """Remove source locations"""
        removed = 0
        for path in self.args.paths:
            resolved_path = self._resolve_network_path(path)
            if not resolved_path:
                continue

            if self.config.remove_source(resolved_path):
                self.ui.print_success(f"Removed source location: {resolved_path.resolve()}")
                removed += 1
            else:
                self.ui.print_warning(f"Location not found: {resolved_path.resolve()}")

        if removed > 0:
            self.config_manager.save(self.config)

        return True

    def _locations_set_target(self):
        """Set target location"""
        path = self.args.path
        resolved_path = self._resolve_network_path(path)
        if not resolved_path:
            return False

        if not resolved_path.exists():
            self.ui.print_error(f"Path does not exist: {resolved_path}")
            return False
        if not resolved_path.is_dir():
            self.ui.print_error(f"Not a directory: {resolved_path}")
            return False

        # Safety check: target cannot be inside any source location
        target_resolved = str(resolved_path.resolve())
        for source_location in self.config.source_locations:
            source_path = pathlib.Path(source_location).resolve()

            # Check if target is inside source
            if target_resolved.startswith(str(source_path) + os.sep) or target_resolved == str(source_path):
                self.ui.print_error(f"Target cannot be inside source location: {source_path}")
                self.ui.print_error("This would create circular references and data loss risks.")
                return False

            # Check if source is inside target (also problematic)
            if str(source_path).startswith(target_resolved + os.sep):
                self.ui.print_error(f"Source location {source_path} is inside target location.")
                self.ui.print_error("Remove the source location first, or choose a different target.")
                return False

        self.config.set_target(resolved_path)
        self.config_manager.save(self.config)
        self.ui.print_success(f"Set target location: {resolved_path.resolve()}")

        return True

    def _locations_set_reference(self):
        """Set reference location"""
        path = self.args.path
        resolved_path = self._resolve_network_path(path)
        if not resolved_path:
            return False

        if not resolved_path.exists():
            self.ui.print_error(f"Path does not exist: {resolved_path}")
            return False
        if not resolved_path.is_dir():
            self.ui.print_error(f"Not a directory: {resolved_path}")
            return False

        # Safety check: reference cannot be same as target or any source
        reference_resolved = str(resolved_path.resolve())

        # Check against target
        if self.config.target_location:
            target_path = pathlib.Path(self.config.target_location).resolve()
            if reference_resolved == str(target_path):
                self.ui.print_error("Reference location cannot be the same as target location")
                return False

        # Check against sources
        for source_location in self.config.source_locations:
            source_path = pathlib.Path(source_location).resolve()
            if reference_resolved == str(source_path):
                self.ui.print_error("Reference location cannot be the same as a source location")
                return False

        self.config.set_reference(resolved_path)
        self.config_manager.save(self.config)
        self.ui.print_success(f"Set reference location: {resolved_path.resolve()}")

        return True

    def _locations_clear(self):
        """Clear all locations"""
        if self.ui.confirm("\nThis will clear all configured locations. Continue?"):
            self.config = MonosisConfig.default()
            self.config_manager.save(self.config)
            self.ui.print_success("All locations cleared")
        else:
            self.ui.print_info("Clear operation cancelled")

        return True

    def _locations_list(self):
        """List configured locations"""
        # Remove the "Configured Locations:" header

        if self.config.source_locations:
            self.ui.print_info("Source locations:")
            for location in self.config.source_locations:
                self.ui.console.print(f"  {location}")
        else:
            self.ui.print_warning("\nNo source locations configured")

        if self.config.target_location:
            # Check if target location exists
            target_path = pathlib.Path(self.config.target_location)
            if target_path.exists():
                self.ui.print_info("\nTarget location:")
                self.ui.console.print(f"  {self.config.target_location}")
            else:
                self.ui.print_info("\nTarget location:")
                self.ui.print_error(f"  {self.config.target_location} (does not exist)")
        else:
            self.ui.print_warning("\nNo target location configured")

        if self.config.reference_location:
            self.ui.print_info("\nReference location (read-only):")
            self.ui.console.print(f"  {self.config.reference_location}")
        else:
            self.ui.print_warning("\nNo reference location configured")

        return True

    def cmd_scan(self):
        """Scan configured locations for duplicates using two-phase approach"""
        # Check if locations are configured
        if not self.config.source_locations:
            self.ui.print_error("No source locations configured. Use 'monosis locations add' first.")
            return False

        # Phase 1: Discovery
        self.ui.print_info("\nPhase 1: File Discovery\n")
        file_inventory = self._discover_files()
        if not file_inventory:
            return False

        # Phase 2: Duplicate Detection
        self.ui.print_info("\nPhase 2: Duplicate Detection\n")
        duplicates = self._detect_duplicates(file_inventory)

        # Update scan time and save results
        self.config.update_scan_time()
        self.config_manager.save(self.config)

        # Save and show results
        self._save_scan_results_v2(duplicates, file_inventory)
        self._show_scan_summary_v2(duplicates, file_inventory)

        return True

    def _discover_files(self) -> dict:
        """Phase 1: Discover all files and build inventory"""
        # Get all locations to scan (sources + reference)
        locations_to_scan = []

        # Add source locations
        for location in self.config.source_locations:
            path = pathlib.Path(location)
            if path.exists() and path.is_dir():
                locations_to_scan.append(("source", location, path))
            else:
                self.ui.print_warning(f"Skipping invalid source location: {location}")

        # Add reference location if configured
        if self.config.reference_location:
            path = pathlib.Path(self.config.reference_location)
            if path.exists() and path.is_dir():
                locations_to_scan.append(("reference", self.config.reference_location, path))
            else:
                self.ui.print_warning(f"Skipping invalid reference location: {self.config.reference_location}")

        if not locations_to_scan:
            self.ui.print_error("No valid locations found")
            return {}

        extensions = set(self.args.extensions) if self.args.extensions else None
        file_inventory = {"locations": {}, "files_by_location": defaultdict(list), "total_files": 0, "total_size": 0}

        # Scan each location with progress updates
        with self.ui.create_activity_progress() as progress:
            discovery_task = progress.add_task("Discovering files...", total=None)

            for location_type, location_str, location_path in locations_to_scan:
                location_files = []
                location_size = 0
                # pattern removed since we use os.walk or iterdir directly

                # Show current location being scanned
                location_display = location_str.replace(str(pathlib.Path.home()), "~")

                # Scan this location using os.walk for better performance
                if self.args.recursive:
                    # Recursive scan using os.walk
                    for root, _dirs, files in os.walk(location_path):
                        for filename in files:
                            file_path = pathlib.Path(root) / filename

                            if extensions:
                                ext = file_path.suffix.lower().lstrip(".")
                                if ext not in extensions:
                                    continue

                            try:
                                stat_result = file_path.stat()
                                file_size = stat_result.st_size
                                file_mtime = stat_result.st_mtime

                                file_info = {
                                    "path": file_path,
                                    "size": file_size,
                                    "mtime": file_mtime,
                                    "location_type": location_type,
                                    "location": location_str,
                                }

                                location_files.append(file_info)
                                location_size += file_size

                                # Update progress periodically (every 1000 files)
                                if len(location_files) % 1000 == 0:
                                    total_files_so_far = file_inventory["total_files"] + len(location_files)
                                    total_size_so_far = file_inventory["total_size"] + location_size

                                    # Format size for display
                                    if total_size_so_far >= 1024**3:
                                        size_str = f"{total_size_so_far / (1024**3):.1f} GiB"
                                    elif total_size_so_far >= 1024**2:
                                        size_str = f"{total_size_so_far / (1024**2):.1f} MiB"
                                    elif total_size_so_far >= 1024:
                                        size_str = f"{total_size_so_far / 1024:.1f} KiB"
                                    else:
                                        size_str = f"{total_size_so_far} B"

                                    progress.update(
                                        discovery_task,
                                        description=f"Scanning {location_display} → {total_files_so_far:,} files, {size_str}",
                                    )

                            except OSError:
                                continue  # Skip files we can't stat
                else:
                    # Non-recursive scan - just list files in the directory
                    try:
                        for item in location_path.iterdir():
                            if not item.is_file():
                                continue

                            if extensions:
                                ext = item.suffix.lower().lstrip(".")
                                if ext not in extensions:
                                    continue

                            try:
                                stat_result = item.stat()
                                file_size = stat_result.st_size
                                file_mtime = stat_result.st_mtime

                                file_info = {
                                    "path": item,
                                    "size": file_size,
                                    "mtime": file_mtime,
                                    "location_type": location_type,
                                    "location": location_str,
                                }

                                location_files.append(file_info)
                                location_size += file_size

                                # Update progress periodically (every 1000 files)
                                if len(location_files) % 1000 == 0:
                                    total_files_so_far = file_inventory["total_files"] + len(location_files)
                                    total_size_so_far = file_inventory["total_size"] + location_size

                                    # Format size for display
                                    if total_size_so_far >= 1024**3:
                                        size_str = f"{total_size_so_far / (1024**3):.1f} GiB"
                                    elif total_size_so_far >= 1024**2:
                                        size_str = f"{total_size_so_far / (1024**2):.1f} MiB"
                                    elif total_size_so_far >= 1024:
                                        size_str = f"{total_size_so_far / 1024:.1f} KiB"
                                    else:
                                        size_str = f"{total_size_so_far} B"

                                    progress.update(
                                        discovery_task,
                                        description=f"Scanning {location_display} → {total_files_so_far:,} files, {size_str}",
                                    )

                            except OSError:
                                continue  # Skip files we can't stat
                    except OSError:
                        self.ui.print_warning(f"Cannot access directory: {location_path}")

                # Store location info
                file_inventory["locations"][location_str] = {
                    "type": location_type,
                    "files": location_files,
                    "count": len(location_files),
                    "size": location_size,
                }

                file_inventory["files_by_location"][location_str] = location_files
                file_inventory["total_files"] += len(location_files)
                file_inventory["total_size"] += location_size

                # Final update for this location
                total_size_gib = file_inventory["total_size"] / (1024**3)
                if total_size_gib >= 1:
                    size_str = f"{total_size_gib:.1f} GiB"
                elif file_inventory["total_size"] >= 1024**2:
                    size_str = f"{file_inventory['total_size'] / (1024**2):.1f} MiB"
                elif file_inventory["total_size"] >= 1024:
                    size_str = f"{file_inventory['total_size'] / 1024:.1f} KiB"
                else:
                    size_str = f"{file_inventory['total_size']} B"

                progress.update(
                    discovery_task,
                    description=f"Completed {location_display} → Total: {file_inventory['total_files']:,} files, {size_str}",
                )

        # Show discovery summary in table format

        from rich.table import Table

        table = Table(show_header=False, box=None, pad_edge=False)
        table.add_column("Type", width=2, justify="center")
        table.add_column("Location", style="white")
        table.add_column("Files", justify="right", style="white")
        table.add_column("Size", justify="right", style="white")

        for location_str, info in file_inventory["locations"].items():
            location_type_symbol = "📁"  # folder emoji for all locations
            location_type_text = "source" if info["type"] == "source" else "reference"
            
            size_gib = info["size"] / (1024**3)
            if size_gib >= 1:
                size_str = f"{size_gib:.1f} GiB"
            elif info["size"] >= 1024**2:
                size_str = f"{info['size'] / (1024**2):.1f} MiB"
            elif info["size"] >= 1024:
                size_str = f"{info['size'] / 1024:.1f} KiB"
            else:
                size_str = f"{info['size']} B"

            table.add_row(location_type_symbol, location_str, f"{info['count']:,}", f"{size_str} ({location_type_text})")

        self.ui.console.print(table)

        return file_inventory

    def _detect_duplicates(self, file_inventory: dict) -> dict:
        """Phase 2: Detect duplicates with smart filtering"""
        if file_inventory["total_files"] == 0:
            return {}

        # Collect all files for duplicate detection
        all_files = []
        for location_files in file_inventory["files_by_location"].values():
            all_files.extend([file_info["path"] for file_info in location_files])

        # Group by size first (optimization)
        self.ui.console.print("Grouping files by size...", style="white dim")
        size_groups = defaultdict(list)
        for file_info in [item for sublist in file_inventory["files_by_location"].values() for item in sublist]:
            size_groups[file_info["size"]].append(file_info)

        # Only process files that have potential duplicates
        potential_duplicates = []
        unique_files = 0
        for _size, files in size_groups.items():
            if len(files) > 1:
                potential_duplicates.extend([f["path"] for f in files])
            else:
                unique_files += 1

        self.ui.console.print(
            f"Size analysis: {unique_files:,} unique sizes, {len(potential_duplicates):,} files need hashing",
            style="white dim",
        )

        if not potential_duplicates:
            self.ui.print_success("No potential duplicates found (all files have unique sizes)")
            return {}

        # Hash potential duplicates with progress

        self.ui.console.print()  # Empty line before hashing

        with self.ui.create_progress() as progress:
            hash_task = progress.add_task("Computing hashes...", total=len(potential_duplicates))
            duplicates = {}

            # Process files in batches and cache results incrementally
            batch_size = 100
            for i in range(0, len(potential_duplicates), batch_size):
                batch = potential_duplicates[i : i + batch_size]
                batch_duplicates = self.duplicate_detector.find_duplicates(batch)
                duplicates.update(batch_duplicates)

                # Cache the hashes for files in this batch
                self._cache_batch_hashes(batch, batch_duplicates)

                processed = min(i + batch_size, len(potential_duplicates))

                # Update progress
                progress.update(hash_task, completed=processed)

        return duplicates

    def _save_scan_results_v2(self, duplicates: dict, file_inventory: dict):
        """Save enhanced scan results with location categorization"""
        # Calculate enhanced statistics
        total_files = sum(len(paths) for paths in duplicates.values())
        total_size = 0
        wasted_space = 0

        # Categorize duplicates by location type
        enhanced_duplicates = {}
        location_analysis = {
            "new_files": [],  # In sources only
            "backed_up_files": [],  # In sources AND reference
            "source_duplicates": [],  # Multiple copies in sources
        }

        for hash_val, paths in duplicates.items():
            if not paths:
                continue

            file_size = paths[0].stat().st_size
            total_size += file_size * len(paths)
            wasted_space += file_size * (len(paths) - 1)

            # Categorize by location types
            source_files = []
            reference_files = []

            for file_path in paths:
                # Find which location this file belongs to
                for location_str, location_info in file_inventory["locations"].items():
                    if str(file_path).startswith(location_str):
                        if location_info["type"] == "source":
                            source_files.append(str(file_path))
                        elif location_info["type"] == "reference":
                            reference_files.append(str(file_path))
                        break

            # Determine category
            has_source = len(source_files) > 0
            has_reference = len(reference_files) > 0
            multiple_sources = len(source_files) > 1

            category = "unknown"
            if has_source and not has_reference:
                category = "source_only"
                if multiple_sources:
                    location_analysis["source_duplicates"].extend(source_files)
                else:
                    location_analysis["new_files"].extend(source_files)
            elif has_source and has_reference:
                category = "backed_up"
                location_analysis["backed_up_files"].extend(source_files)
            elif has_reference and not has_source:
                category = "reference_only"

            enhanced_duplicates[hash_val] = {
                "files": [str(p) for p in paths],
                "source_files": source_files,
                "reference_files": reference_files,
                "category": category,
                "file_size": file_size,
            }

        # Prepare enhanced data
        data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "locations": {
                "sources": [loc for loc, info in file_inventory["locations"].items() if info["type"] == "source"],
                "reference": [loc for loc, info in file_inventory["locations"].items() if info["type"] == "reference"],
            },
            "discovery": {
                "total_files": file_inventory["total_files"],
                "total_size_gb": file_inventory["total_size"] / (1024**3),
                "location_details": {
                    loc: {"count": info["count"], "size_gb": info["size"] / (1024**3), "type": info["type"]}
                    for loc, info in file_inventory["locations"].items()
                },
            },
            "summary": {
                "duplicate_groups": len(duplicates),
                "total_duplicates": total_files,
                "total_size_gb": total_size / (1024**3),
                "wasted_space_gb": wasted_space / (1024**3),
                "new_files_count": len(location_analysis["new_files"]),
                "backed_up_files_count": len(location_analysis["backed_up_files"]),
                "source_duplicates_count": len(location_analysis["source_duplicates"]),
            },
            "duplicates": enhanced_duplicates,
            "location_analysis": location_analysis,
        }

        # Save to file
        with self.scan_results_file.open("w") as f:
            json.dump(data, f, indent=2)

    def _show_scan_summary_v2(self, duplicates: dict, file_inventory: dict):
        """Show enhanced scan summary with location-based categorization"""
        if not duplicates:
            self.ui.print_success("\nNo duplicates found!")
            return

        # Calculate categorized statistics
        new_files = 0
        backed_up_files = 0
        source_duplicates = 0
        total_wasted_space = 0

        for paths in duplicates.values():
            if not paths:
                continue

            file_size = paths[0].stat().st_size
            source_count = 0
            reference_count = 0

            for file_path in paths:
                for location_str, location_info in file_inventory["locations"].items():
                    if str(file_path).startswith(location_str):
                        if location_info["type"] == "source":
                            source_count += 1
                        elif location_info["type"] == "reference":
                            reference_count += 1
                        break

            # Categorize this duplicate group
            if source_count > 0 and reference_count == 0:
                if source_count > 1:
                    source_duplicates += source_count
                    total_wasted_space += file_size * (source_count - 1)
                else:
                    new_files += 1
            elif source_count > 0 and reference_count > 0:
                backed_up_files += source_count
                total_wasted_space += file_size * source_count  # All source copies are redundant

        # Show categorized summary
        self.ui.print_success("\n=== Scan Complete ===")
        self.ui.print_info(f"Found {len(duplicates):,} groups of duplicates")

        if new_files > 0:
            self.ui.print_info(f"📝 New files (need backup): {new_files}")
        if backed_up_files > 0:
            self.ui.print_success(f"✅ Already backed up: {backed_up_files} (can remove from sources)")
        if source_duplicates > 0:
            self.ui.print_warning(f"🔄 Source duplicates: {source_duplicates} (can consolidate)")

        if total_wasted_space > 0:
            self.ui.print_info(f"💾 Recoverable space: {total_wasted_space / (1024**3):.2f} GB")

        self.ui.print_info("\nRun 'monosis analyze' for detailed breakdown")
        if backed_up_files > 0 or source_duplicates > 0:
            self.ui.print_info("Run 'monosis analyze --scope /path' to focus on specific directories")

    def cmd_analyze(self):
        """Analyze scan results and show detailed duplicate information"""
        # Load scan results
        if not self.scan_results_file.exists():
            self.ui.print_error("No scan results found. Run 'monosis scan' first.")
            return False

        with self.scan_results_file.open() as f:
            data = json.load(f)

        self.ui.print_header("Duplicate Analysis", f"Scan performed: {data['timestamp']}")

        # Calculate location-based statistics
        location_stats = self._analyze_location_stats(data)

        # Show location statistics
        self.ui.print_info("\n=== Location Statistics ===")
        for location, stats in location_stats.items():
            self.ui.print_info(f"\n{location}:")
            self.ui.console.print(f"  Total files: {stats['total_files']}")
            self.ui.console.print(f"  Unique files: {stats['unique_files']}")
            self.ui.console.print(f"  Within-location duplicates: {stats['within_duplicates']}")
            self.ui.console.print(f"  Cross-location duplicates: {stats['cross_duplicates']}")
            self.ui.console.print(f"  Wasted space: {stats['wasted_space_gb']:.2f} GB")

        # Show duplicate groups
        self.ui.print_info("\n=== Duplicate Groups ===")
        duplicate_groups = data["duplicates"]

        # Sort by wasted space
        sorted_groups = sorted(
            duplicate_groups.items(),
            key=lambda x: len(x[1]) * pathlib.Path(x[1][0]).stat().st_size if x[1] else 0,
            reverse=True,
        )

        # Show top 10 duplicate groups
        for i, (_hash_val, file_paths) in enumerate(sorted_groups[:10], 1):
            if file_paths:
                file_size = pathlib.Path(file_paths[0]).stat().st_size
                wasted = file_size * (len(file_paths) - 1)

                self.ui.console.print(f"\n{i}. {len(file_paths)} copies, {wasted / (1024**3):.2f} GB wasted")

                # Group by location
                by_location = defaultdict(list)
                for fp in file_paths:
                    for location in self.config.source_locations:
                        if fp.startswith(location):
                            by_location[location].append(fp)
                            break

                for location, paths in by_location.items():
                    self.ui.console.print(f"  {location}: {len(paths)} copies")

        self.ui.print_info("\nRun 'monosis consolidate' to copy unique files to target location")

        return True

    def cmd_consolidate(self):
        """Consolidate unique files to target location"""
        # Check configuration
        if not self.config.target_location:
            self.ui.print_error("No target location configured. Use 'monosis locations target' first.")
            return False

        # Load scan results
        if not self.scan_results_file.exists():
            self.ui.print_error("No scan results found. Run 'monosis scan' first.")
            return False

        self.ui.print_info("Consolidation functionality coming soon...")

        # TODO: Implement consolidation logic
        # 1. Build list of unique files (one per hash)
        # 2. Choose best source for each (most metadata, newest, etc.)
        # 3. Copy to target maintaining directory structure
        # 4. Verify copies
        # 5. Update consolidation timestamp

        return True

    def cmd_clean_sources(self):
        """Clean verified duplicates from source locations"""
        # Safety checks
        if not self.config.last_consolidation:
            self.ui.print_error("No consolidation has been performed. Run 'monosis consolidate' first.")
            return False

        self.ui.print_info("Source cleaning functionality coming soon...")

        # TODO: Implement source cleaning logic
        # 1. Verify all files exist in target
        # 2. Show exact files to be deleted
        # 3. Require explicit confirmation
        # 4. Create deletion log
        # 5. Delete files

        return True

    def cmd_status(self):
        """Show configuration and scan status"""
        # Show locations
        self._locations_list()

        # Check cache database
        self.ui.print_info("\nCache Status")
        if self.cache_db.exists():
            cache_size = self.cache_db.stat().st_size
            if cache_size >= 1024**3:
                cache_size_str = f"{cache_size / (1024**3):.1f} GiB"
            elif cache_size >= 1024**2:
                cache_size_str = f"{cache_size / (1024**2):.1f} MiB"
            elif cache_size >= 1024:
                cache_size_str = f"{cache_size / 1024:.1f} KiB"
            else:
                cache_size_str = f"{cache_size} B"
            
            with sqlite3.connect(self.cache_db) as conn:
                cursor = conn.execute("SELECT COUNT(*) FROM file_hashes")
                count = cursor.fetchone()[0]
                self.ui.console.print(f"  Number of hashes: {count:,} ({cache_size_str})", style="white dim")
        else:
            self.ui.console.print("  No cache database found", style="white dim")

        # Check scan results
        if self.scan_results_file.exists():
            with self.scan_results_file.open() as f:
                data = json.load(f)
                self.ui.console.print(f"  Last scan: {data['timestamp']}", style="white dim")
                self.ui.console.print(f"  Duplicate groups: {data['summary']['duplicate_groups']:,}", style="white dim")
                self.ui.console.print(f"  Total duplicates: {data['summary']['total_duplicates']:,}", style="white dim")
                # Handle both old 'wasted_space_gb' and new 'wasted_space_gib' keys
                wasted_space_key = "wasted_space_gib" if "wasted_space_gib" in data["summary"] else "wasted_space_gb"
                wasted_gib = data["summary"][wasted_space_key]
                if wasted_gib >= 1:
                    wasted_str = f"{wasted_gib:.2f} GiB"
                elif wasted_gib * (1024**3) >= 1024**2:
                    wasted_str = f"{wasted_gib * (1024**3) / (1024**2):.1f} MiB"
                elif wasted_gib * (1024**3) >= 1024:
                    wasted_str = f"{wasted_gib * (1024**3) / 1024:.1f} KiB"
                else:
                    wasted_str = f"{int(wasted_gib * (1024**3))} B"
                self.ui.console.print(f"  Wasted space: {wasted_str}", style="white dim")
        else:
            self.ui.console.print("  No scan results found", style="white dim")

        if self.config.last_consolidation:
            self.ui.console.print(f"  Last consolidation: {self.config.last_consolidation}", style="white dim")

        self.ui.console.print()  # Empty line at the end
        return True

    def cmd_clean(self):
        """Clean cache and results"""
        if hasattr(self.args, "cache_only") and self.args.cache_only:
            return self._clean_cache_only()
        
        if self.ui.confirm("\nThis will delete all cached data and scan results. Continue?"):
            cache_deleted = False
            results_deleted = False
            
            if self.cache_db.exists():
                # Clear in-memory cache and force garbage collection
                self.duplicate_detector._hash_cache.clear()
                import gc
                gc.collect()
                
                try:
                    self.cache_db.unlink()
                    self.ui.print_success("Cache database deleted")
                    cache_deleted = True
                except PermissionError:
                    self.ui.print_error("Cannot delete cache database - it may be in use")
                    cache_deleted = False

            if self.scan_results_file.exists():
                self.scan_results_file.unlink()
                self.ui.print_success("Scan results deleted")
                results_deleted = True
            
            if cache_deleted or results_deleted:
                self.ui.print_success("Clean operation completed")
            else:
                self.ui.print_info("No files to clean")
        else:
            self.ui.print_info("Clean operation cancelled")

        return True
    
    def _clean_cache_only(self):
        """Clean only the hash cache database"""
        if self.cache_db.exists():
            cache_size = self.cache_db.stat().st_size
            if cache_size >= 1024**3:
                cache_size_str = f"{cache_size / (1024**3):.1f} GiB"
            elif cache_size >= 1024**2:
                cache_size_str = f"{cache_size / (1024**2):.1f} MiB"
            elif cache_size >= 1024:
                cache_size_str = f"{cache_size / 1024:.1f} KiB"
            else:
                cache_size_str = f"{cache_size} B"
            
            if self.ui.confirm(f"\nThis will delete the hash cache database ({cache_size_str}). Continue?"):
                # Force close any open connections and clear the in-memory cache
                self.duplicate_detector._hash_cache.clear()
                
                try:
                    # Try to close any lingering SQLite connections
                    import gc
                    gc.collect()  # Force garbage collection to close connections
                    
                    self.cache_db.unlink()
                    self.ui.print_success("Hash cache cleared")
                    self.ui.print_info("Scan results preserved")
                except PermissionError:
                    self.ui.print_error("Cannot delete cache database - it may be in use by another process")
                    self.ui.print_info("Try closing any other instances of monosis and run the command again")
                    return False
            else:
                self.ui.print_info("Cache clean cancelled")
        else:
            self.ui.print_info("No cache database found")
            
        return True

    def _save_scan_results(self, duplicates: dict, scanned_paths: list[pathlib.Path], file_paths_by_location: dict):
        """Save scan results to file with location information"""
        # Calculate summary statistics
        total_files = sum(len(paths) for paths in duplicates.values())
        total_size = 0
        wasted_space = 0

        # Enhance duplicate groups with location information
        enhanced_duplicates = {}

        for hash_val, paths in duplicates.items():
            if paths:
                file_size = paths[0].stat().st_size
                total_size += file_size * len(paths)
                wasted_space += file_size * (len(paths) - 1)

                # Determine which locations contain each file
                locations = set()
                for file_path in paths:
                    for location in self.config.source_locations:
                        if str(file_path).startswith(location):
                            locations.add(location)
                            break

                enhanced_duplicates[hash_val] = {
                    "files": [str(p) for p in paths],
                    "locations": list(locations),
                    "is_cross_location": len(locations) > 1,
                }

        # Prepare data
        data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "scanned_paths": [str(p) for p in scanned_paths],
            "source_locations": self.config.source_locations,
            "summary": {
                "duplicate_groups": len(duplicates),
                "total_duplicates": total_files,
                "total_size_gb": total_size / (1024**3),
                "wasted_space_gb": wasted_space / (1024**3),
            },
            "duplicates": enhanced_duplicates,
        }

        # Save to file
        with self.scan_results_file.open("w") as f:
            json.dump(data, f, indent=2)

    def _show_scan_summary(self, duplicates: dict, file_paths_by_location: dict):
        """Show scan summary with location breakdown"""
        if not duplicates:
            self.ui.print_success("\nNo duplicates found!")
            return

        # Calculate statistics
        total_files = sum(len(paths) for paths in duplicates.values())
        total_size = 0
        wasted_space = 0
        within_location_count = 0
        cross_location_count = 0

        for _hash_val, paths in duplicates.items():
            if paths:
                file_size = paths[0].stat().st_size
                total_size += file_size * len(paths)
                wasted_space += file_size * (len(paths) - 1)

                # Check if cross-location duplicate
                locations = set()
                for file_path in paths:
                    for location in self.config.source_locations:
                        if str(file_path).startswith(location):
                            locations.add(location)
                            break

                if len(locations) > 1:
                    cross_location_count += 1
                else:
                    within_location_count += 1

        # Show summary
        self.ui.print_success("\nScan complete!")
        self.ui.print_info(f"Found {len(duplicates)} groups of duplicates")
        self.ui.print_info(f"  Within-location groups: {within_location_count}")
        self.ui.print_info(f"  Cross-location groups: {cross_location_count}")
        self.ui.print_info(f"Total duplicate files: {total_files}")
        self.ui.print_info(f"Wasted space: {wasted_space / (1024**3):.2f} GB")

        self.ui.print_info("\nRun 'monosis analyze' for detailed analysis")

    def _analyze_location_stats(self, data: dict) -> dict:
        """Analyze statistics per location"""
        stats = {}

        for location in data["source_locations"]:
            stats[location] = {
                "total_files": 0,
                "unique_files": 0,
                "within_duplicates": 0,
                "cross_duplicates": 0,
                "wasted_space_gb": 0.0,
            }

        # Process duplicate groups
        for _hash_val, group_data in data["duplicates"].items():
            files = group_data["files"]

            if files:
                file_size = pathlib.Path(files[0]).stat().st_size

                # Count files per location
                for file_path in files:
                    for location in data["source_locations"]:
                        if file_path.startswith(location):
                            stats[location]["total_files"] += 1

                            if group_data["is_cross_location"]:
                                stats[location]["cross_duplicates"] += 1
                            else:
                                stats[location]["within_duplicates"] += 1

                            # Add wasted space (except for first copy)
                            location_count = sum(1 for f in files if f.startswith(location))
                            if location_count > 1:
                                stats[location]["wasted_space_gb"] += (location_count - 1) * file_size / (1024**3)
                            break

        # Calculate unique files
        for _location, location_stats in stats.items():
            total = location_stats["total_files"]
            dups = location_stats["within_duplicates"] + location_stats["cross_duplicates"]
            location_stats["unique_files"] = total - dups

        return stats

    def _cache_batch_hashes(self, file_paths: list[pathlib.Path], duplicates: dict):
        """Cache hashes for a batch of files to SQLite database"""
        import time

        current_time = time.time()
        cache_entries = []

        # For each file in the batch, get its hash and prepare cache entry
        for file_path in file_paths:
            try:
                stat = file_path.stat()
                file_size = stat.st_size
                mtime = stat.st_mtime

                # Find the hash for this file from the duplicates result
                file_hash = None
                for hash_val, paths in duplicates.items():
                    if file_path in paths:
                        file_hash = hash_val
                        break

                # If we found a hash, prepare cache entry
                if file_hash:
                    cache_entries.append(
                        (
                            str(file_path),
                            file_size,
                            mtime,
                            None,  # quick_hash
                            file_hash,  # full_hash
                            current_time,
                        )
                    )

            except OSError:
                continue  # Skip files we can't stat

        # Batch insert into cache database
        if cache_entries:
            with sqlite3.connect(self.cache_db) as conn:
                conn.executemany(
                    """
                    INSERT OR REPLACE INTO file_hashes 
                    (file_path, file_size, mtime, quick_hash, full_hash, last_scan)
                    VALUES (?, ?, ?, ?, ?, ?)
                """,
                    cache_entries,
                )
                conn.commit()


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Monosis - Intelligent file deduplication with location management",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Location-Based Workflow:
  1. monosis locations add /path1 /path2      # Add source locations
  2. monosis locations target /target         # Set consolidation target  
  3. monosis locations reference /backup      # Set read-only reference
  4. monosis scan                             # Scan all locations
  5. monosis analyze                          # Analyze duplicates
  6. monosis consolidate --skip-referenced    # Copy unique files to target
  7. monosis clean-sources --interactive      # Remove verified duplicates

Examples:
  monosis locations add ~/Photos ~/Documents/Pictures
  monosis locations target ~/Temp/Consolidated
  monosis locations reference /mnt/backup-server
  monosis scan --recursive
  monosis analyze
  monosis consolidate --dry-run
  monosis status
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands", required=True)

    # Locations command
    locations_parser = subparsers.add_parser("locations", help="Manage source and target locations")
    locations_subparsers = locations_parser.add_subparsers(dest="location_command", help="Location commands")

    # locations add
    add_parser = locations_subparsers.add_parser("add", help="Add source locations")
    add_parser.add_argument("paths", nargs="+", type=pathlib.Path, help="Directories to add as sources")

    # locations remove
    remove_parser = locations_subparsers.add_parser("remove", help="Remove source locations")
    remove_parser.add_argument("paths", nargs="+", type=pathlib.Path, help="Directories to remove from sources")

    # locations target
    target_parser = locations_subparsers.add_parser("target", help="Set target location")
    target_parser.add_argument("path", type=pathlib.Path, help="Directory to use as consolidation target")

    # locations reference
    reference_parser = locations_subparsers.add_parser("reference", help="Set reference location (read-only)")
    reference_parser.add_argument("path", type=pathlib.Path, help="Directory to use as read-only reference")

    # locations clear
    locations_subparsers.add_parser("clear", help="Clear all locations")

    # locations list (default)
    locations_subparsers.add_parser("list", help="List configured locations")

    # Scan command
    scan_parser = subparsers.add_parser("scan", help="Scan configured locations for duplicates")
    scan_parser.add_argument("-r", "--recursive", action="store_true", help="Scan subdirectories recursively")
    scan_parser.add_argument("--no-cache", action="store_true", help="Don't use cached hashes")
    scan_parser.add_argument("--quick", action="store_true", help="Quick scan (partial hashes only)")
    scan_parser.add_argument("-e", "--extensions", nargs="+", help="Only scan files with these extensions")

    # Analyze command
    analyze_parser = subparsers.add_parser("analyze", help="Analyze scan results with location details")
    analyze_parser.add_argument("--scope", type=pathlib.Path, help="Focus analysis on specific directory")

    # Consolidate command
    consolidate_parser = subparsers.add_parser("consolidate", help="Consolidate unique files to target")
    consolidate_parser.add_argument(
        "--interactive", action="store_true", help="Interactive mode for conflict resolution"
    )
    consolidate_parser.add_argument(
        "--strategy",
        choices=["newest", "oldest", "largest", "most-metadata"],
        default="newest",
        help="Auto-resolution strategy",
    )
    consolidate_parser.add_argument("--dry-run", action="store_true", help="Show what would be done without copying")

    # Clean sources command
    clean_parser = subparsers.add_parser("clean-sources", help="Remove verified duplicates from sources")
    clean_parser.add_argument("--dry-run", action="store_true", help="Show what would be deleted without removing")
    clean_parser.add_argument("--interactive", action="store_true", help="Confirm each deletion individually")

    # Status command
    subparsers.add_parser("status", help="Show configuration and statistics")

    # Clean command
    clean_parser = subparsers.add_parser("clean", help="Clean cache and results")
    clean_parser.add_argument("--cache-only", action="store_true", help="Only clear hash cache, preserve scan results")

    args = parser.parse_args()

    # Initialize Monosis
    app = Monosis(args)

    # Show configuration
    app.show_configuration()

    # Execute command
    success = False
    if args.command == "locations":
        success = app.cmd_locations()
    elif args.command == "scan":
        success = app.cmd_scan()
    elif args.command == "analyze":
        success = app.cmd_analyze()
    elif args.command == "consolidate":
        success = app.cmd_consolidate()
    elif args.command == "clean-sources":
        success = app.cmd_clean_sources()
    elif args.command == "status":
        success = app.cmd_status()
    elif args.command == "clean":
        success = app.cmd_clean()

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
