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
        self.duplicate_detector = DuplicateDetector(hash_algorithm="md5")
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

    def show_configuration(self):
        """Show current configuration"""
        config = {}

        # Always show locations
        config["Source locations"] = self.config.source_locations if self.config.source_locations else ["undefined"]
        config["Target location"] = self.config.target_location or "undefined"

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
            if self.args.location_command == "clear":
                return self._locations_clear()

        # Default: list locations
        return self._locations_list()

    def _locations_add(self):
        """Add source locations"""
        added = 0
        for path in self.args.paths:
            if not path.exists():
                self.ui.print_error(f"Path does not exist: {path}")
                continue
            if not path.is_dir():
                self.ui.print_error(f"Not a directory: {path}")
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

            if self.config.add_source(path):
                self.ui.print_success(f"Added source location: {path.resolve()}")
                added += 1
            else:
                self.ui.print_warning(f"Location already configured: {path.resolve()}")

        if added > 0:
            self.config_manager.save(self.config)

        return True

    def _locations_remove(self):
        """Remove source locations"""
        removed = 0
        for path in self.args.paths:
            if self.config.remove_source(path):
                self.ui.print_success(f"Removed source location: {path.resolve()}")
                removed += 1
            else:
                self.ui.print_warning(f"Location not found: {path.resolve()}")

        if removed > 0:
            self.config_manager.save(self.config)

        return True

    def _locations_set_target(self):
        """Set target location"""
        path = self.args.path
        if not path.exists():
            self.ui.print_error(f"Path does not exist: {path}")
            return False
        if not path.is_dir():
            self.ui.print_error(f"Not a directory: {path}")
            return False

        # Safety check: target cannot be inside any source location
        target_resolved = str(path.resolve())
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

        self.config.set_target(path)
        self.config_manager.save(self.config)
        self.ui.print_success(f"Set target location: {path.resolve()}")

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
        self.ui.print_header("Configured Locations")

        if self.config.source_locations:
            self.ui.print_info("\nSource locations:")
            for i, location in enumerate(self.config.source_locations, 1):
                self.ui.console.print(f"  {i}. {location}")
        else:
            self.ui.print_warning("\nNo source locations configured")

        if self.config.target_location:
            self.ui.print_info(f"\nTarget location:\n  {self.config.target_location}")
        else:
            self.ui.print_warning("\nNo target location configured")

        return True

    def cmd_scan(self):
        """Scan configured locations for duplicates"""
        # Check if locations are configured
        if not self.config.source_locations:
            self.ui.print_error("No source locations configured. Use 'monosis locations add' first.")
            return False

        # Get valid paths
        valid_paths = []
        for location in self.config.source_locations:
            path = pathlib.Path(location)
            if path.exists() and path.is_dir():
                valid_paths.append(path)
            else:
                self.ui.print_warning(f"Skipping invalid location: {location}")

        if not valid_paths:
            self.ui.print_error("No valid source locations found")
            return False

        self.ui.print_info(f"\nScanning {len(valid_paths)} locations...")

        # Find all files
        extensions = set(self.args.extensions) if self.args.extensions else None
        file_paths_by_location = defaultdict(list)
        all_file_paths = []

        with self.ui.create_progress() as progress:
            scan_task = progress.add_task("Finding files...", total=None)

            for directory in valid_paths:
                location_files = []
                pattern = "**/*" if self.args.recursive else "*"

                for file_path in directory.glob(pattern):
                    if not file_path.is_file():
                        continue

                    if extensions:
                        ext = file_path.suffix.lower().lstrip(".")
                        if ext not in extensions:
                            continue

                    location_files.append(file_path)
                    all_file_paths.append(file_path)
                    progress.update(scan_task, description=f"Found {len(all_file_paths)} files...")

                file_paths_by_location[str(directory)] = location_files

            progress.update(scan_task, completed=len(all_file_paths), total=len(all_file_paths))

        if not all_file_paths:
            self.ui.print_warning("No files found to scan")
            return True

        self.ui.print_success(f"Found {len(all_file_paths)} files to analyze")

        # Find duplicates with progress
        self.ui.print_info("\nAnalyzing files for duplicates...")
        duplicates = self.duplicate_detector.find_duplicates(all_file_paths)

        # Update scan time
        self.config.update_scan_time()
        self.config_manager.save(self.config)

        # Save results with location information
        self._save_scan_results(duplicates, valid_paths, file_paths_by_location)

        # Show summary
        self._show_scan_summary(duplicates, file_paths_by_location)

        return True

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
        self.ui.print_header("Monosis Status")

        # Show locations
        self._locations_list()

        # Check cache database
        if self.cache_db.exists():
            with sqlite3.connect(self.cache_db) as conn:
                cursor = conn.execute("SELECT COUNT(*) FROM file_hashes")
                count = cursor.fetchone()[0]
                self.ui.print_info(f"\nCached file hashes: {count}")
        else:
            self.ui.print_info("\nNo cache database found")

        # Check scan results
        if self.scan_results_file.exists():
            with self.scan_results_file.open() as f:
                data = json.load(f)
                self.ui.print_info(f"\nLast scan: {data['timestamp']}")
                self.ui.print_info(f"Duplicate groups: {data['summary']['duplicate_groups']}")
                self.ui.print_info(f"Total duplicates: {data['summary']['total_duplicates']}")
                self.ui.print_info(f"Wasted space: {data['summary']['wasted_space_gb']:.2f} GB")
        else:
            self.ui.print_info("\nNo scan results found")

        if self.config.last_consolidation:
            self.ui.print_info(f"\nLast consolidation: {self.config.last_consolidation}")

        return True

    def cmd_clean(self):
        """Clean cache and results"""
        if self.ui.confirm("\nThis will delete all cached data and scan results. Continue?"):
            if self.cache_db.exists():
                self.cache_db.unlink()
                self.ui.print_success("Cache database deleted")

            if self.scan_results_file.exists():
                self.scan_results_file.unlink()
                self.ui.print_success("Scan results deleted")

            self.ui.print_success("Cache cleaned successfully")
        else:
            self.ui.print_info("Clean operation cancelled")

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


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Monosis - Intelligent file deduplication with location management",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Location-Based Workflow:
  1. monosis locations add /path1 /path2    # Add source locations
  2. monosis locations target /target       # Set consolidation target
  3. monosis scan                           # Scan all locations
  4. monosis analyze                        # Analyze duplicates
  5. monosis consolidate                    # Copy unique files to target
  6. monosis clean-sources --interactive    # Remove verified duplicates

Examples:
  monosis locations add ~/Photos ~/Documents/Pictures ~/Backup
  monosis locations target ~/Consolidated
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
    subparsers.add_parser("analyze", help="Analyze scan results with location details")

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
    subparsers.add_parser("clean", help="Clean cache and results")

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
