#!/usr/bin/env python3
"""
Monosis - Ancient Greek μόνωσις (reduction to unity)

A high-performance file deduplication tool that reduces many scattered copies
to their essential singular form. Designed to handle hundreds of thousands of
files efficiently with multi-stage processing and intelligent caching.

Features:
- Location-based management (multiple sources, target, reference)
- Multi-stage hashing pipeline for performance
- Persistent caching between runs with shared .kosmos infrastructure
- Intelligent file filtering (size, patterns)
- Check specific paths for duplicates

Workflow:
1. locations - Configure source, target, and reference directories
2. scan      - Build hash index of configured locations
3. check     - Check specific paths for duplicates
4. status    - View configuration and cache statistics
5. clean     - Clean cache files (index, hashes, or both)
"""

import argparse
import fnmatch
import gc
import json
import os
import pathlib
import pickle
import signal
import sqlite3
import sys
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Optional

from rich.table import Table

# Local imports
from auxiliary import format_bytes, format_path_for_display
from console_ui import ConsoleUI
from duplicate_detector import DuplicateDetector
from file_indexer import FileIndexer
from file_operations import FileOperations
from monosis_config import ConfigManager, MonosisConfig

# Fix Windows console encoding for Unicode characters
if sys.platform == "win32":
    import io

    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")


class Monosis:
    """Main application class for Monosis deduplication"""

    def __init__(self, args):
        self.args = args
        self.ui = ConsoleUI()
        self.duplicate_detector = DuplicateDetector(hash_algorithm="xxhash64")
        self.file_operations = FileOperations()
        self._shutdown_requested = False

        # Initialize configuration manager (now uses .kosmos)
        self.config_manager = ConfigManager()
        self.config = self.config_manager.load()

        # Cache configuration - now uses shared .kosmos directory
        self.cache_dir = self.config_manager.config_dir
        self.cache_db = self.config_manager.get_cache_db_path()  # Shared hash cache
        self.scan_results_file = self.cache_dir / "monosis_scan_results.json"
        self.index_cache_file = self.cache_dir / "monosis_file_index.pkl"

        # Initialize file indexer
        self.file_indexer = FileIndexer(
            cache_file=self.index_cache_file,
            ignore_patterns=self.config.ignore_patterns,
        )

        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        if hasattr(signal, "SIGTERM"):
            signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        self._shutdown_requested = True
        self.ui.print_warning("\nShutdown requested... saving current progress and exiting.")

        # Ensure cache directory exists
        self.cache_dir.mkdir(exist_ok=True)

        # Initialize cache database
        self._init_cache_db()

        # Load existing cache into DuplicateDetector
        self._load_cache_into_detector()

    def _init_cache_db(self):
        """Initialize SQLite cache database - now handled by shared config"""
        # Database initialization is now done in kosmos_config.init_shared_cache_db
        # Just verify it exists and set the path for the duplicate detector

    def _load_cache_into_detector(self):
        """Load cached hashes into the duplicate detector for faster lookups"""
        if not self.cache_db.exists():
            return

        # Just pass the cache database path to the detector
        # Let it handle loading on demand
        self.duplicate_detector._cache_db_path = self.cache_db

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

                if hasattr(self.args, "strategy"):
                    config["Strategy"] = self.args.strategy

            elif self.args.command == "clean-sources":
                config["Dry run"] = "Yes" if self.args.dry_run else "No"
                config["Interactive"] = "Yes" if self.args.interactive else "No"

        self.ui.show_configuration(config)
        self.ui.console.print()  # Empty line after configuration

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
        self.ui.print_info("\nLocations")

        # Collect all locations with their types
        all_locations = []

        # Add source locations
        for location in self.config.source_locations:
            all_locations.append((location, "source"))

        # Add target location
        if self.config.target_location:
            target_path = pathlib.Path(self.config.target_location)
            if target_path.exists():
                all_locations.append((self.config.target_location, "target"))
            else:
                all_locations.append((self.config.target_location, "target (missing)"))

        # Add reference location
        if self.config.reference_location:
            all_locations.append((self.config.reference_location, "reference"))

        if all_locations:
            # Find the maximum location length for alignment
            max_location_len = max(len(loc[0]) for loc in all_locations)

            # Print each location with aligned type
            for location, loc_type in all_locations:
                # Right-align the type after the location
                padding = max_location_len - len(location) + 2
                self.ui.console.print(f"  {location}{' ' * padding}{loc_type}", style="white dim")
        else:
            self.ui.console.print("  No locations configured", style="white dim")

        return True

    def cmd_scan(self):
        """Scan configured locations for duplicates using two-phase approach"""
        # Check if locations are configured
        if not self.config.source_locations and not self.config.reference_location:
            self.ui.print_error("No locations configured. Use 'monosis locations add' or 'monosis locations reference' first.")
            return False

        # Phase 1: Discovery
        self.ui.print_info("Phase 1: File Discovery\n")

        # Check if we should use cached index
        if hasattr(self.args, "use_cached_index") and self.args.use_cached_index:
            self.ui.console.print("Loading cached file index...", style="white dim")

            # Get current locations for validation
            current_locations = set(self.config.source_locations)
            if self.config.reference_location:
                current_locations.add(self.config.reference_location)

            # Load cache with validation
            file_inventory = self.file_indexer.load_cache(
                current_locations, validation_callback=lambda msg: self.ui.console.print(msg, style="white dim")
            )

            if file_inventory:
                self.ui.print_success("Using cached file index - skipping discovery phase")
            else:
                self.ui.print_info("No valid cached index found, performing fresh discovery")
                file_inventory = self._discover_files()
        else:
            # Default: always refresh (full discovery)
            file_inventory = self._discover_files()
        if not file_inventory:
            return False

        # Phase 2: Hash Computation and Database Creation
        self.ui.print_info("\nPhase 2: Hash Computation\n")
        self._compute_all_hashes(file_inventory)

        # Update scan time and save index
        self.config.update_scan_time()
        self.config_manager.save(self.config)

        # Save file index for future use
        self.file_indexer.save_cache(file_inventory)

        # Show scan summary
        self._show_scan_summary_with_hashes(file_inventory)

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

        # Scan files with progress callbacks
        with self.ui.create_activity_progress() as progress:
            discovery_task = progress.add_task("Discovering files...", total=None)

            # Define progress callback for file discovery
            def progress_callback(location_str, _location_files, total_files, final=False):
                location_display = format_path_for_display(location_str)

                if final:
                    progress.update(
                        discovery_task,
                        description=f"Completed {location_display} → {total_files:,} files",
                    )
                else:
                    progress.update(
                        discovery_task,
                        description=f"Scanning {location_display} → {total_files:,} files",
                    )

            # Set progress callback and shutdown flag
            self.file_indexer.progress_callback = progress_callback
            self.file_indexer.shutdown_requested = lambda: self._shutdown_requested

            # Discover files
            file_inventory = self.file_indexer.discover_files(locations_to_scan, recursive=self.args.recursive)

            # Check if shutdown was requested during discovery
            if self._shutdown_requested:
                self.ui.print_info("File discovery interrupted. Partial results saved.")
                return {}

        # Show discovery summary in table format

        table = Table(show_header=False, box=None, pad_edge=False)
        table.add_column("Type", width=2, justify="center")
        table.add_column("Location", style="white")
        table.add_column("Files", justify="right", style="white")
        table.add_column("Size", justify="right", style="white", min_width=10)
        table.add_column("Type", style="white dim", min_width=12)

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

            table.add_row(location_type_symbol, location_str, f"{info['count']:,}", size_str, location_type_text)

        self.ui.console.print(table)

        # Cache the file inventory for future use
        self.ui.console.print("\nCaching file index for future scans...", style="white dim")
        self.file_indexer.save_cache(file_inventory)

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

        # Hash potential duplicates with progress (parallel)
        self.ui.console.print()  # Empty line before hashing

        duplicates = self._parallel_hash_files(potential_duplicates)

        return duplicates

    def _parallel_hash_files(self, potential_duplicates: list[pathlib.Path]) -> dict:
        """Hash files in parallel using ThreadPoolExecutor with incremental cache saves"""

        # Thread-safe progress tracking
        progress_lock = threading.Lock()
        total_db_cache_hits = 0
        completed_files = 0
        file_hashes = {}  # Thread-safe dict for results
        pending_cache_entries = []  # Batch cache entries before saving

        def hash_single_file(file_path: pathlib.Path) -> tuple[pathlib.Path, Optional[str], bool]:
            """Hash a single file and return (path, hash, was_cached)"""
            try:
                # Check database cache first
                cached_hash = None
                if self.duplicate_detector._cache_db_path and self.duplicate_detector._cache_db_path.exists():
                    cached_hash = self.duplicate_detector._check_db_cache(file_path)

                if cached_hash:
                    return (file_path, cached_hash, True)

                # Calculate hash
                file_hash = self.duplicate_detector.calculate_file_hash(file_path)
                return (file_path, file_hash, False)
            except Exception:
                return (file_path, None, False)

        # Start parallel hashing
        with self.ui.create_progress() as progress:
            hash_task = progress.add_task("Computing hashes...", total=len(potential_duplicates))

            with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
                # Submit all hash jobs
                future_to_file = {
                    executor.submit(hash_single_file, file_path): file_path for file_path in potential_duplicates
                }

                # Process completed hashes as they finish
                for future in as_completed(future_to_file):
                    # Check for shutdown signal
                    if self._shutdown_requested:
                        # Save any pending cache entries before shutdown
                        if pending_cache_entries:
                            self._save_cache_batch(pending_cache_entries)
                            pending_cache_entries.clear()
                        # Cancel remaining futures and break
                        for remaining_future in future_to_file:
                            remaining_future.cancel()
                        break

                    file_path, file_hash, was_cached = future.result()

                    with progress_lock:
                        completed_files += 1

                        if file_hash:
                            # Track cache hits
                            if was_cached:
                                total_db_cache_hits += 1
                            else:
                                # Add to pending cache entries for batch save
                                try:
                                    stat = file_path.stat()
                                    pending_cache_entries.append((file_path, stat, file_hash))
                                except OSError:
                                    pass  # Skip files we can't stat

                            # Store hash
                            if file_hash not in file_hashes:
                                file_hashes[file_hash] = []
                            file_hashes[file_hash].append(file_path)

                            # Check if we should save cache batch
                            computed_hashes = len(pending_cache_entries)
                            if computed_hashes >= self.config.cache_batch_size:
                                self._save_cache_batch(pending_cache_entries)
                                pending_cache_entries.clear()

                        # Update progress
                        progress.update(
                            hash_task,
                            completed=completed_files,
                            description=f"Computing hashes... ({total_db_cache_hits:,} from cache)",
                        )

        # Save any remaining cache entries
        if pending_cache_entries:
            self._save_cache_batch(pending_cache_entries)

        # Check if shutdown was requested
        if self._shutdown_requested:
            self.ui.print_info("Saved progress before shutdown.")
            sys.exit(0)

        # Return only groups with duplicates (2+ files with same hash)
        return {hash_val: paths for hash_val, paths in file_hashes.items() if len(paths) > 1}

    def _save_cache_batch(self, cache_entries: list[tuple]):
        """Save a batch of cache entries to database"""
        if not cache_entries:
            return

        current_time = time.time()
        db_entries = []

        for file_path, stat_info, file_hash in cache_entries:
            db_entries.append(
                (
                    str(file_path),
                    stat_info.st_size,
                    stat_info.st_mtime,
                    None,  # quick_hash
                    file_hash,
                    "xxhash64",
                    "monosis",
                    current_time,
                )
            )

        try:
            with sqlite3.connect(self.cache_db) as conn:
                conn.executemany(
                    """
                    INSERT OR REPLACE INTO file_hashes 
                    (file_path, file_size, mtime, quick_hash, full_hash, hash_algorithm, tool_name, last_scan)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    db_entries,
                )
                conn.commit()
        except sqlite3.Error:
            # Ignore database errors - don't break the hashing process
            pass

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

            try:
                file_size = paths[0].stat().st_size
                total_size += file_size * len(paths)
                wasted_space += file_size * (len(paths) - 1)
            except OSError:
                # File was deleted between hashing and results processing
                # Try other files in the group, or skip this group if all are gone
                file_size = None
                for path in paths:
                    try:
                        file_size = path.stat().st_size
                        total_size += file_size * len(paths)
                        wasted_space += file_size * (len(paths) - 1)
                        break
                    except OSError:
                        continue

                # If all files in this duplicate group are gone, skip it
                if file_size is None:
                    continue

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
        """Show scan summary focused on file discovery results"""
        # Calculate total duplicate groups and files
        duplicate_groups = len(duplicates)
        duplicate_files = sum(len(paths) for paths in duplicates.values())

        # Show scan completion
        self.ui.print_success("\nScan complete!")

        # Show file discovery summary
        self.ui.print_info(f"Total files indexed: {file_inventory['total_files']:,}")
        if duplicate_groups > 0:
            self.ui.print_info(f"Files with matching hashes: {duplicate_files:,} in {duplicate_groups:,} groups")

        # Show location breakdown
        for location_str, location_info in file_inventory["locations"].items():
            location_display = format_path_for_display(location_str)
            file_count = location_info["count"]
            size = location_info["size"]
            size_str = format_bytes(size)

            location_type = location_info["type"].capitalize()
            self.ui.print_info(f"  {location_type}: {file_count:,} files, {size_str} in {location_display}")

        self.ui.print_info("\nUse 'monosis check <path>' to analyze specific files for duplicates")

    def _show_scan_summary_index_only(self, file_inventory: dict):
        """Show scan summary for index-only results (no duplicate analysis)"""
        # Show scan completion
        self.ui.print_success("\nScan complete!")

        # Show file discovery summary
        self.ui.print_info(f"Total files indexed: {file_inventory['total_files']:,}")

        # Show location breakdown
        for location_str, location_info in file_inventory["locations"].items():
            location_display = format_path_for_display(location_str)
            file_count = location_info["count"]
            size = location_info["size"]
            size_str = format_bytes(size)

            location_type = location_info["type"].capitalize()
            self.ui.print_info(f"  {location_type}: {file_count:,} files, {size_str} in {location_display}")

        self.ui.print_info("\nIndex saved. Use 'monosis check <path>' to find duplicates.")

    def _compute_all_hashes(self, file_inventory: dict):
        """Phase 2: Compute hashes for all discovered files"""
        # Collect all file paths from inventory
        all_files = []
        for location_info in file_inventory["locations"].values():
            for file_info in location_info["files"]:
                all_files.append(file_info["path"])

        if not all_files:
            self.ui.print_info("No files to hash")
            return

        # Use existing parallel hashing with incremental cache saves
        self._parallel_hash_files(all_files)

    def _show_scan_summary_with_hashes(self, file_inventory: dict):
        """Show scan summary including hash database status"""
        # Show scan completion
        self.ui.print_success("\nScan complete!")

        # Show file discovery summary
        self.ui.print_info(f"Total files indexed: {file_inventory['total_files']:,}")

        # Show hash database status
        if self.cache_db.exists():
            try:
                with sqlite3.connect(self.cache_db) as conn:
                    cursor = conn.execute("SELECT COUNT(*) FROM file_hashes")
                    hash_count = cursor.fetchone()[0]
                    self.ui.print_info(f"Hashes computed: {hash_count:,}")
            except Exception:
                pass

        # Show location breakdown
        for location_str, location_info in file_inventory["locations"].items():
            location_display = format_path_for_display(location_str)
            file_count = location_info["count"]
            size = location_info["size"]
            size_str = format_bytes(size)

            location_type = location_info["type"].capitalize()
            self.ui.print_info(f"  {location_type}: {file_count:,} files, {size_str} in {location_display}")

        self.ui.print_info("\nDatabase ready. Use 'monosis check <path>' for fast duplicate lookups.")

    def cmd_check(self):
        """Check specific path for duplicates (files) or similar folders (directories)"""
        # Verify database exists
        if not self.cache_db.exists():
            self.ui.print_error("No hash database found. Run 'monosis scan' first to build the database.")
            return False

        # Get the path to check
        check_path = self.args.path.resolve()

        # Validate path
        if not check_path.exists():
            self.ui.print_error(f"Path does not exist: {check_path}")
            return False

        # Check if it's a file or directory
        if check_path.is_file():
            return self._check_file_duplicates(check_path)
        return self._check_folder_similarity(check_path)

    def _check_file_duplicates(self, file_path: pathlib.Path):
        """Check a single file for exact duplicates"""
        self.ui.print_info(f"Checking file: {file_path}")

        # Filter by size if needed
        min_size = (
            self.args.min_size if hasattr(self.args, "min_size") and self.args.min_size else self.config.min_file_size
        )

        try:
            if file_path.stat().st_size < min_size:
                self.ui.print_warning("File too small, skipping")
                return True
        except OSError:
            self.ui.print_error("Cannot access file")
            return False

        # Find duplicates for this file
        duplicates_found = self._find_duplicates_for_files([file_path], file_path.parent)

        # Display results
        self._display_check_results(duplicates_found, file_path.parent)
        return True

    def _check_folder_similarity(self, folder_path: pathlib.Path):
        """Check a folder for similar folders based on content overlap"""
        self.ui.print_info(f"Checking folder: {folder_path}")

        # Get folder inventory (all files with their hashes)
        folder_inventory = self._get_folder_inventory(folder_path)

        if not folder_inventory:
            self.ui.print_warning("No files found in folder")
            return True

        self.ui.print_info(f"Files in folder: {len(folder_inventory):,}")

        # Find similar folders
        similar_folders = self._find_similar_folders(folder_path, folder_inventory)

        # Display results
        self._display_folder_similarity_results(folder_path, folder_inventory, similar_folders)
        return True

    def _get_folder_inventory(self, folder_path: pathlib.Path) -> dict[str, dict]:
        """Get inventory of all files in a folder with their hashes"""
        inventory = {}
        min_size = self.config.min_file_size

        with self.ui.create_progress() as progress:
            task = progress.add_task("Building folder inventory...", total=None)
            file_count = 0

            for root, _dirs, files in os.walk(folder_path):
                for filename in files:
                    file_path = pathlib.Path(root) / filename

                    try:
                        stat_info = file_path.stat()
                        if stat_info.st_size < min_size:
                            continue

                        # Get relative path within the folder for comparison
                        rel_path = file_path.relative_to(folder_path)

                        # Get or calculate hash
                        file_hash = None
                        if self.duplicate_detector._cache_db_path and self.duplicate_detector._cache_db_path.exists():
                            file_hash = self.duplicate_detector._check_db_cache(file_path)

                        if not file_hash:
                            file_hash = self.duplicate_detector.calculate_file_hash(file_path)
                            if file_hash:
                                self._cache_single_file_hash(file_path, file_hash)

                        if file_hash:
                            inventory[str(rel_path)] = {
                                "hash": file_hash,
                                "size": stat_info.st_size,
                                "full_path": str(file_path),
                            }

                        file_count += 1
                        if file_count % 100 == 0:
                            progress.update(task, description=f"Building folder inventory... {file_count} files")

                    except OSError:
                        continue

            progress.update(task, description=f"Completed inventory: {len(inventory)} files")

        return inventory

    def _find_similar_folders(
        self, check_folder: pathlib.Path, folder_inventory: dict[str, dict]
    ) -> list[tuple[str, float, dict]]:
        """Find folders in database locations that are similar to the check folder"""
        # Get folder candidates with file counts for smart filtering
        folder_candidates = self._get_database_folder_candidates()

        # Pre-filter candidates by size similarity (within 50% file count range)
        check_file_count = len(folder_inventory)
        min_files = max(1, int(check_file_count * 0.3))  # At least 30% of files
        max_files = int(check_file_count * 3.0)  # At most 300% of files

        filtered_candidates = []
        for folder_path, file_count in folder_candidates:
            if min_files <= file_count <= max_files:
                filtered_candidates.append(folder_path)

        self.ui.print_info(
            f"Comparing against {len(filtered_candidates)} potential folders (filtered from {len(folder_candidates)})"
        )

        similar_folders = []

        with self.ui.create_progress() as progress:
            task = progress.add_task("Comparing with database folders...", total=len(filtered_candidates))

            for candidate_folder in filtered_candidates:
                progress.update(task, advance=1)

                # Skip if it's the same folder
                try:
                    if pathlib.Path(candidate_folder).resolve() == check_folder.resolve():
                        continue
                except (OSError, ValueError):
                    continue

                # Get inventory for candidate folder (from cache)
                candidate_inventory = self._get_folder_inventory_from_db(candidate_folder)

                if not candidate_inventory:
                    continue

                # Quick size check before expensive similarity calculation
                size_ratio = len(candidate_inventory) / len(folder_inventory)
                if size_ratio < 0.5 or size_ratio > 2.0:
                    continue

                # Calculate similarity
                similarity_score = self._calculate_folder_similarity(folder_inventory, candidate_inventory)

                # Only include folders with >80% similarity
                if similarity_score > 0.80:
                    similar_folders.append((candidate_folder, similarity_score, candidate_inventory))

        # Sort by similarity score (highest first)
        similar_folders.sort(key=lambda x: x[1], reverse=True)
        return similar_folders

    def _get_database_folder_candidates(self, min_files: int = 5) -> list[tuple[str, int]]:
        """Get folder paths from database with file counts for smart filtering"""
        folders = {}

        try:
            with sqlite3.connect(self.cache_db) as conn:
                # Get all file paths and group by folder
                cursor = conn.execute("SELECT file_path FROM file_hashes")
                for (file_path_str,) in cursor.fetchall():
                    folder_path = str(pathlib.Path(file_path_str).parent)
                    folders[folder_path] = folders.get(folder_path, 0) + 1
        except sqlite3.Error:
            pass

        # Filter folders with minimum file count and sort by file count (larger first)
        return sorted(
            [(folder, count) for folder, count in folders.items() if count >= min_files],
            key=lambda x: x[1],
            reverse=True,
        )

    def _get_folder_inventory_from_db(self, folder_path: str) -> dict[str, dict]:
        """Get folder inventory from database (faster than filesystem scan)"""
        inventory = {}
        folder_p = pathlib.Path(folder_path)

        # Normalize folder path for database query
        normalized_folder = str(folder_p).replace("/", "\\") if os.name == "nt" else str(folder_p).replace("\\", "/")

        try:
            with sqlite3.connect(self.cache_db) as conn:
                # More efficient query - use GLOB for better path matching
                cursor = conn.execute(
                    "SELECT file_path, file_size, full_hash FROM file_hashes WHERE file_path GLOB ?",
                    (f"{normalized_folder}*",),
                )

                for file_path_str, file_size, file_hash in cursor.fetchall():
                    # Quick string check before path operations
                    if not file_path_str.startswith(normalized_folder):
                        continue

                    try:
                        file_path = pathlib.Path(file_path_str)
                        rel_path = file_path.relative_to(folder_p)
                        inventory[str(rel_path)] = {"hash": file_hash, "size": file_size, "full_path": file_path_str}
                    except ValueError:
                        continue

        except sqlite3.Error:
            pass

        return inventory

    def _calculate_folder_similarity(self, inventory1: dict[str, dict], inventory2: dict[str, dict]) -> float:
        """Calculate similarity score between two folder inventories"""
        if not inventory1 or not inventory2:
            return 0.0

        # Create sets of file hashes for comparison
        hashes1 = {info["hash"] for info in inventory1.values()}
        hashes2 = {info["hash"] for info in inventory2.values()}

        # Calculate Jaccard similarity (intersection / union)
        intersection = len(hashes1.intersection(hashes2))
        union = len(hashes1.union(hashes2))

        if union == 0:
            return 0.0

        return intersection / union

    def _display_folder_similarity_results(
        self,
        check_folder: pathlib.Path,
        folder_inventory: dict[str, dict],
        similar_folders: list[tuple[str, float, dict]],
    ):
        """Display folder similarity results"""
        if not similar_folders:
            self.ui.print_success("No similar folders found")
            return

        total_size = sum(info["size"] for info in folder_inventory.values())
        self.ui.print_info(f"Found {len(similar_folders)} similar folders")

        for index, (folder_path, similarity_score, candidate_inventory) in enumerate(similar_folders, 1):
            folder_display = format_path_for_display(folder_path)
            candidate_size = sum(info["size"] for info in candidate_inventory.values())

            self.ui.console.print(f"\n[{index}] {folder_display}", style="white")
            self.ui.console.print(
                f"    Similarity: {similarity_score:.1%} | Files: {len(candidate_inventory):,} | Size: {format_bytes(candidate_size)}",
                style="white dim",
            )

            # Show shared files count
            shared_hashes = {info["hash"] for info in folder_inventory.values()}.intersection(
                {info["hash"] for info in candidate_inventory.values()}
            )
            only_in_check = len(folder_inventory) - len(shared_hashes)
            only_in_candidate = len(candidate_inventory) - len(shared_hashes)

            self.ui.console.print(
                f"    Shared: {len(shared_hashes):,} files | Missing from candidate: {only_in_check:,} | Extra in candidate: {only_in_candidate:,}",
                style="white dim",
            )

        self.ui.console.print(
            f"\nChecked folder: {len(folder_inventory):,} files, {format_bytes(total_size)}", style="white dim"
        )

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

        # Check file index cache
        cache_stats = self.file_indexer.get_cache_stats()
        if cache_stats:
            # Format timestamp for display
            cache_timestamp = cache_stats.get("timestamp", "Unknown")
            if cache_timestamp != "Unknown":
                try:
                    dt = datetime.fromisoformat(cache_timestamp.replace("Z", "+00:00"))
                    cache_timestamp = dt.strftime("%Y-%m-%d %H:%M:%S")
                except (ValueError, AttributeError):
                    pass
            self.ui.console.print(
                f"  Indexed files: {cache_stats['total_files']:,} (from {cache_timestamp})", style="white dim"
            )
        else:
            self.ui.console.print("  Indexed files: 0", style="white dim")

        # Check scan results
        if self.scan_results_file.exists():
            with self.scan_results_file.open() as f:
                data = json.load(f)
                # Format timestamp for display
                timestamp = data["timestamp"]
                try:
                    dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                    timestamp = dt.strftime("%Y-%m-%d %H:%M:%S")
                except (ValueError, AttributeError):
                    pass
                self.ui.console.print(f"  Last scan: {timestamp}", style="white dim")
        else:
            self.ui.console.print("  No scan results found", style="white dim")

        if self.config.last_consolidation:
            self.ui.console.print(f"  Last consolidation: {self.config.last_consolidation}", style="white dim")

        self.ui.console.print()  # Empty line at end
        return True

    def cmd_clean(self):
        """Clean cache and results"""
        # Check for specific clean options
        if hasattr(self.args, "hashes_only") and self.args.hashes_only:
            return self._clean_hashes_only()
        if hasattr(self.args, "index_only") and self.args.index_only:
            return self._clean_index_only()
        if hasattr(self.args, "cache_only") and self.args.cache_only:
            return self._clean_cache_only()

        # Default: clean everything
        if self.ui.confirm("\nThis will delete all cached data and scan results. Continue?"):
            cache_deleted = False
            results_deleted = False

            if self.cache_db.exists():
                # Clear in-memory cache and force garbage collection
                self.duplicate_detector._hash_cache.clear()
                gc.collect()

                try:
                    self.cache_db.unlink()
                    self.ui.print_success("Hash cache deleted")
                    cache_deleted = True
                except PermissionError:
                    self.ui.print_error("Cannot delete cache database - it may be in use")
                    cache_deleted = False

            if self.scan_results_file.exists():
                self.scan_results_file.unlink()
                self.ui.print_success("Scan results deleted")
                results_deleted = True

            if self.file_indexer.clear_cache():
                self.ui.print_success("File index cache deleted")
                cache_deleted = True

            if cache_deleted or results_deleted:
                self.ui.print_success("Clean operation completed")
            else:
                self.ui.print_info("No files to clean")
        else:
            self.ui.print_info("Clean operation cancelled")

        return True

    def _clean_hashes_only(self):
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

            if self.ui.confirm(f"\nThis will delete only the hash cache ({cache_size_str}). Continue?"):
                # Force close any open connections and clear the in-memory cache
                self.duplicate_detector._hash_cache.clear()

                try:
                    # Try to close any lingering SQLite connections
                    gc.collect()  # Force garbage collection to close connections

                    self.cache_db.unlink()
                    self.ui.print_success("Hash cache deleted")
                except PermissionError:
                    self.ui.print_error("Cannot delete cache database - it may be in use by another process")
                    self.ui.print_info("Try closing any other instances of monosis and run the command again")
                    return False
            else:
                self.ui.print_info("Hash cache clean cancelled")
        else:
            self.ui.print_info("No hash cache database found")

        return True

    def _clean_index_only(self):
        """Clean only the file index cache"""
        cache_stats = self.file_indexer.get_cache_stats()
        if cache_stats:
            if self.ui.confirm(
                f"\nThis will delete the file index cache ({cache_stats['total_files']:,} files). Continue?"
            ):
                if self.file_indexer.clear_cache():
                    self.ui.print_success("File index cache deleted")
                else:
                    self.ui.print_error("Failed to delete file index cache")
            else:
                self.ui.print_info("Index cache clean cancelled")
        else:
            self.ui.print_info("No file index cache found")

        return True

    def _clean_cache_only(self):
        """Clean both hash cache and file index cache (preserves scan results)"""
        files_to_clean = []
        total_size = 0

        # Check hash cache
        if self.cache_db.exists():
            cache_size = self.cache_db.stat().st_size
            files_to_clean.append(("hash cache", cache_size))
            total_size += cache_size

        # Check index cache
        cache_stats = self.file_indexer.get_cache_stats()
        if cache_stats:
            index_size = cache_stats["size"]
            files_to_clean.append(("file index cache", index_size))
            total_size += index_size

        if not files_to_clean:
            self.ui.print_info("No cache files found")
            return True

        # Format total size
        if total_size >= 1024**3:
            total_size_str = f"{total_size / (1024**3):.1f} GiB"
        elif total_size >= 1024**2:
            total_size_str = f"{total_size / (1024**2):.1f} MiB"
        elif total_size >= 1024:
            total_size_str = f"{total_size / 1024:.1f} KiB"
        else:
            total_size_str = f"{total_size} B"

        if self.ui.confirm(
            f"\nThis will delete cache files ({total_size_str}). Scan results will be preserved. Continue?"
        ):
            deleted_files = []

            # Clean hash cache
            if self.cache_db.exists():
                # Force close any open connections and clear the in-memory cache
                self.duplicate_detector._hash_cache.clear()

                try:
                    gc.collect()  # Force garbage collection to close connections
                    self.cache_db.unlink()
                    deleted_files.append("hash cache")
                except PermissionError:
                    self.ui.print_error("Cannot delete hash cache - it may be in use by another process")

            # Clean index cache
            if self.file_indexer.clear_cache():
                deleted_files.append("file index cache")

            if deleted_files:
                self.ui.print_success(f"Deleted: {', '.join(deleted_files)}")
                self.ui.print_info("Scan results preserved")
            else:
                self.ui.print_error("No cache files could be deleted")
        else:
            self.ui.print_info("Cache clean cancelled")

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

    def _find_duplicates_for_files(self, files_to_check: list[pathlib.Path], check_path: pathlib.Path) -> dict:
        """Find duplicates for specified files by querying hash database"""
        duplicates = {}

        with self.ui.create_progress() as progress:
            task = progress.add_task("Checking for duplicates...", total=len(files_to_check))

            for file_path in files_to_check:
                progress.update(task, advance=1)

                try:
                    # Get file hash (calculate if not cached)
                    file_hash = None

                    # First check if we have it in cache
                    if self.duplicate_detector._cache_db_path and self.duplicate_detector._cache_db_path.exists():
                        file_hash = self.duplicate_detector._check_db_cache(file_path)

                    # Calculate if not cached
                    if not file_hash:
                        file_hash = self.duplicate_detector.calculate_file_hash(file_path)
                        # Cache it for future use
                        if file_hash:
                            self._cache_single_file_hash(file_path, file_hash)

                    if not file_hash:
                        continue

                    # Query database for other files with same hash
                    other_files = self._query_hash_in_database(file_hash, exclude_path=str(file_path))

                    # Apply external-only filter if requested
                    if other_files and hasattr(self.args, "external_only") and self.args.external_only:
                        # Filter out files that are within check_path
                        check_path_str = str(check_path)
                        other_files = [f for f in other_files if not f.startswith(check_path_str)]

                    if other_files:
                        duplicates[str(file_path)] = {
                            "hash": file_hash,
                            "size": file_path.stat().st_size,
                            "duplicates": other_files,
                        }

                except OSError:
                    continue  # Skip files we can't access

        return duplicates

    def _query_hash_in_database(self, file_hash: str, exclude_path: Optional[str] = None) -> list[str]:
        """Query database for all files with given hash"""
        results = []

        with sqlite3.connect(self.cache_db) as conn:
            if exclude_path:
                cursor = conn.execute(
                    "SELECT file_path FROM file_hashes WHERE full_hash = ? AND file_path != ?",
                    (file_hash, exclude_path),
                )
            else:
                cursor = conn.execute("SELECT file_path FROM file_hashes WHERE full_hash = ?", (file_hash,))

            results = [row[0] for row in cursor.fetchall()]

        return results

    def _cache_single_file_hash(self, file_path: pathlib.Path, file_hash: str):
        """Cache a single file's hash to database"""
        try:
            stat = file_path.stat()
            with sqlite3.connect(self.cache_db) as conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO file_hashes 
                    (file_path, file_size, mtime, quick_hash, full_hash, hash_algorithm, tool_name, last_scan)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (str(file_path), stat.st_size, stat.st_mtime, None, file_hash, "xxhash64", "monosis", time.time()),
                )
                conn.commit()
        except OSError:
            pass

    def _display_check_results(self, duplicates_found: dict, check_path: pathlib.Path):
        """Display check results with detailed file information and enumeration"""
        if not duplicates_found:
            self.ui.print_success("No duplicates found")
            return

        total_duplicates = sum(len(info["duplicates"]) for info in duplicates_found.values())
        self.ui.print_info(f"Found {len(duplicates_found)} files with {total_duplicates} duplicates")

        for file_index, (file_path, info) in enumerate(duplicates_found.items(), 1):
            file_p = pathlib.Path(file_path)

            # Get file details for the checked file
            try:
                stat_info = file_p.stat()
                size_str = format_bytes(info["size"])
                mtime = datetime.fromtimestamp(stat_info.st_mtime, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            except OSError:
                size_str = format_bytes(info["size"])
                mtime = "unknown"

            # Show the original file
            try:
                display_path = file_p.relative_to(check_path) if check_path in file_p.parents else str(file_p)
            except ValueError:
                display_path = str(file_p)

            self.ui.console.print(f"\n[{file_index}] {display_path}", style="white")
            self.ui.console.print(
                f"    Hash: {info['hash'][:16]}... Size: {size_str} Modified: {mtime}", style="white dim"
            )

            # Show enumerated duplicates with their details
            for dup_index, dup_path in enumerate(info["duplicates"], 1):
                try:
                    dup_p = pathlib.Path(dup_path)
                    dup_stat = dup_p.stat()
                    dup_size_str = format_bytes(dup_stat.st_size)
                    dup_mtime = datetime.fromtimestamp(dup_stat.st_mtime, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

                    # Check if sizes match (they should for identical files)
                    size_match = dup_stat.st_size == info["size"]
                    size_indicator = "✓" if size_match else "✗"

                    self.ui.console.print(f"    {dup_index}. {dup_path}", style="white")
                    self.ui.console.print(
                        f"       Size: {dup_size_str} {size_indicator} Modified: {dup_mtime}", style="white dim"
                    )

                except OSError:
                    self.ui.console.print(f"    {dup_index}. {dup_path} (file not accessible)", style="red dim")

        # Summary
        total_size = sum(info["size"] for info in duplicates_found.values())
        self.ui.console.print(f"\nTotal size of checked files: {format_bytes(total_size)}", style="white dim")

    def _cache_batch_hashes(self, file_paths: list[pathlib.Path], duplicates: dict):
        """Cache hashes for a batch of files to SQLite database"""
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
                # Add hash_algorithm and tool_name to each entry
                cache_entries_with_tool = [
                    (e[0], e[1], e[2], e[3], e[4], "xxhash64", "monosis", e[5]) for e in cache_entries
                ]
                conn.executemany(
                    """
                    INSERT OR REPLACE INTO file_hashes 
                    (file_path, file_size, mtime, quick_hash, full_hash, hash_algorithm, tool_name, last_scan)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    cache_entries_with_tool,
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
  5. monosis check /path/to/check             # Check specific path for duplicates
  7. monosis clean-sources --interactive      # Remove verified duplicates

Examples:
  monosis locations add ~/Photos ~/Documents/Pictures
  monosis locations target ~/Temp/Consolidated
  monosis locations reference /mnt/backup-server
  monosis scan --recursive
  monosis check ~/Downloads --external-only
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
    scan_parser.add_argument(
        "--use-cached-index",
        action="store_true",
        help="Skip file discovery, use cached file index from previous scan",
    )

    # Check command
    check_parser = subparsers.add_parser("check", help="Check specific path for duplicates")
    check_parser.add_argument("path", type=pathlib.Path, help="Path to check (file or directory)")
    check_parser.add_argument("--external-only", action="store_true", help="Only show duplicates outside checked path")
    check_parser.add_argument("--min-size", type=int, help="Minimum file size in bytes to check")

    # Status command
    subparsers.add_parser("status", help="Show configuration and statistics")

    # Clean command
    clean_parser = subparsers.add_parser("clean", help="Clean cache and results")
    clean_group = clean_parser.add_mutually_exclusive_group()
    clean_group.add_argument("--hashes-only", action="store_true", help="Only clear hash cache")
    clean_group.add_argument("--index-only", action="store_true", help="Only clear file index cache")
    clean_group.add_argument(
        "--cache-only", action="store_true", help="Clear both hash and index cache, preserve scan results"
    )

    args = parser.parse_args()

    # Initialize Monosis
    app = Monosis(args)

    # Execute command
    success = False
    if args.command == "locations":
        # Show configuration first for location commands
        app.show_configuration()
        success = app.cmd_locations()
    elif args.command == "scan":
        # Show configuration first for scan
        app.show_configuration()
        success = app.cmd_scan()
    elif args.command == "check":
        # Check doesn't need configuration display
        success = app.cmd_check()
    elif args.command == "status":
        # Status shows its own info, no configuration needed
        success = app.cmd_status()
    elif args.command == "clean":
        # Clean doesn't need configuration display
        success = app.cmd_clean()

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
