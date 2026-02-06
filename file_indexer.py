#!/usr/bin/env python3
"""
File Indexer Module for Monosis

Handles file discovery, indexing, and caching operations.
Provides efficient file inventory management with persistent caching.
"""

import fnmatch
import os
import pathlib
import pickle
from collections import defaultdict
from datetime import datetime, timezone
from typing import Callable, Optional


class FileIndexer:
    """Manages file discovery and indexing with caching support"""

    def __init__(
        self,
        cache_file: pathlib.Path,
        ignore_patterns: Optional[list[str]] = None,
        progress_callback: Optional[Callable] = None,
    ):
        """Initialize file indexer

        Args:
            cache_file: Path to the index cache file
            ignore_patterns: List of glob patterns to ignore
            progress_callback: Optional callback for progress updates
        """
        self.cache_file = cache_file
        self.ignore_patterns = ignore_patterns or []
        self.progress_callback = progress_callback
        self.shutdown_requested = None  # Callable that returns True if shutdown requested

    def discover_files(
        self,
        locations: list[tuple[str, str, pathlib.Path]],
        recursive: bool = True,
    ) -> dict:
        """Discover all files in specified locations

        Args:
            locations: List of (location_type, location_str, location_path) tuples
            recursive: Whether to scan directories recursively

        Returns:
            Dictionary containing file inventory with locations, files, and statistics
        """
        file_inventory = {
            "locations": {},
            "files_by_location": defaultdict(list),
            "total_files": 0,
            "total_size": 0,
        }

        for location_type, location_str, location_path in locations:
            # Check for shutdown before processing each location
            if self.shutdown_requested and self.shutdown_requested():
                break

            location_files = []
            location_size = 0

            # Scan this location
            if recursive:
                # Recursive scan using os.walk
                for root, _dirs, files in os.walk(location_path):
                    # Check for shutdown during scanning
                    if self.shutdown_requested and self.shutdown_requested():
                        break

                    for filename in files:
                        file_path = pathlib.Path(root) / filename

                        file_info = self._process_file(file_path, location_type, location_str)
                        if file_info:
                            location_files.append(file_info)
                            location_size += file_info["size"]

                            # Notify progress periodically
                            if self.progress_callback and len(location_files) % 1000 == 0:
                                total_files_so_far = file_inventory["total_files"] + len(location_files)
                                self.progress_callback(
                                    location_str, len(location_files), total_files_so_far, final=False
                                )
            else:
                # Non-recursive scan
                try:
                    for entry in location_path.iterdir():
                        if entry.is_file():
                            file_info = self._process_file(entry, location_type, location_str)
                            if file_info:
                                location_files.append(file_info)
                                location_size += file_info["size"]

                                # Notify progress periodically
                                if self.progress_callback and len(location_files) % 1000 == 0:
                                    total_files_so_far = file_inventory["total_files"] + len(location_files)
                                    self.progress_callback(
                                        location_str, len(location_files), total_files_so_far, final=False
                                    )
                except (OSError, PermissionError):
                    pass  # Skip inaccessible directories

            # Store location information
            file_inventory["locations"][location_str] = {
                "type": location_type,
                "files": location_files,
                "count": len(location_files),
                "size": location_size,
            }

            file_inventory["files_by_location"][location_str] = location_files
            file_inventory["total_files"] += len(location_files)
            file_inventory["total_size"] += location_size

            # Final progress update for this location
            if self.progress_callback:
                self.progress_callback(location_str, len(location_files), file_inventory["total_files"], final=True)

        return file_inventory

    def _process_file(self, file_path: pathlib.Path, location_type: str, location_str: str) -> Optional[dict]:
        """Process a single file and return its info if it should be indexed

        Args:
            file_path: Path to the file
            location_type: Type of location (source/reference/target)
            location_str: String representation of the location

        Returns:
            File info dictionary or None if file should be ignored
        """
        try:
            stat_result = file_path.stat()
            file_size = stat_result.st_size
            file_mtime = stat_result.st_mtime

            # Apply filtering
            if self._should_ignore_file(file_path):
                return None

            return {
                "path": file_path,
                "size": file_size,
                "mtime": file_mtime,
                "location_type": location_type,
                "location": location_str,
            }
        except (OSError, PermissionError):
            return None

    def _should_ignore_file(self, file_path: pathlib.Path) -> bool:
        """Check if a file should be ignored based on filters

        Args:
            file_path: Path to the file

        Returns:
            True if file should be ignored, False otherwise
        """
        # Check ignore patterns only - no size filtering
        file_str = str(file_path)
        for pattern in self.ignore_patterns:
            if fnmatch.fnmatch(file_str, pattern) or fnmatch.fnmatch(file_path.name, pattern):
                return True

        return False

    def save_cache(self, file_inventory: dict) -> bool:
        """Save file index to cache for future scans

        Args:
            file_inventory: File inventory dictionary to cache

        Returns:
            True if cache was saved successfully, False otherwise
        """
        cache_data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "locations": {},
        }

        for location_str, location_info in file_inventory["locations"].items():
            try:
                location_path = pathlib.Path(location_str)
                # Get directory modification time for invalidation
                dir_mtime = location_path.stat().st_mtime if location_path.exists() else 0

                cache_data["locations"][location_str] = {
                    "type": location_info["type"],
                    "dir_mtime": dir_mtime,
                    "cached_at": cache_data["timestamp"],
                    "files": [
                        {
                            "path": str(file_info["path"]),
                            "size": file_info["size"],
                            "mtime": file_info["mtime"],
                            "location_type": file_info["location_type"],
                            "location": file_info["location"],
                        }
                        for file_info in location_info["files"]
                    ],
                }
            except (OSError, KeyError):
                continue  # Skip problematic locations

        # Save cache to file
        try:
            self.cache_file.parent.mkdir(parents=True, exist_ok=True)
            with self.cache_file.open("wb") as f:
                pickle.dump(cache_data, f, protocol=pickle.HIGHEST_PROTOCOL)
            return True
        except OSError:
            return False

    def load_cache(self, current_locations: set[str], validation_callback: Optional[Callable] = None) -> dict:
        """Load file index from cache if valid

        Args:
            current_locations: Set of current location paths to validate against
            validation_callback: Optional callback for validation messages

        Returns:
            Cached file inventory or empty dict if cache is invalid
        """
        if not self.cache_file.exists():
            return {}

        try:
            with self.cache_file.open("rb") as f:
                cache_data = pickle.load(f)
        except (pickle.UnpicklingError, OSError, EOFError):
            return {}

        # Check if current locations match cached locations
        cached_locations = set(cache_data.get("locations", {}).keys())
        if current_locations != cached_locations:
            if validation_callback:
                validation_callback("Location configuration changed, cache invalid")
            return {}

        # Validate each location's modification time
        file_inventory = {
            "locations": {},
            "files_by_location": defaultdict(list),
            "total_files": 0,
            "total_size": 0,
        }

        for location_str, cached_location in cache_data["locations"].items():
            try:
                location_path = pathlib.Path(location_str)
                current_mtime = location_path.stat().st_mtime if location_path.exists() else 0
                cached_mtime = cached_location.get("dir_mtime", 0)

                # Allow small time differences (file system precision)
                if abs(current_mtime - cached_mtime) > 1.0:
                    if validation_callback:
                        validation_callback(f"Directory changed: {location_str}, cache invalid")
                    return {}

                # Reconstruct file inventory from cache
                location_files = []
                location_size = 0

                for cached_file in cached_location["files"]:
                    file_info = {
                        "path": pathlib.Path(cached_file["path"]),
                        "size": cached_file["size"],
                        "mtime": cached_file["mtime"],
                        "location_type": cached_file["location_type"],
                        "location": cached_file["location"],
                    }
                    location_files.append(file_info)
                    location_size += cached_file["size"]

                file_inventory["locations"][location_str] = {
                    "type": cached_location["type"],
                    "files": location_files,
                    "count": len(location_files),
                    "size": location_size,
                }

                file_inventory["files_by_location"][location_str] = location_files
                file_inventory["total_files"] += len(location_files)
                file_inventory["total_size"] += location_size

            except (OSError, KeyError):
                if validation_callback:
                    validation_callback(f"Error validating cache for: {location_str}")
                return {}

        return file_inventory

    def clear_cache(self) -> bool:
        """Clear the index cache file

        Returns:
            True if cache was cleared successfully, False otherwise
        """
        try:
            if self.cache_file.exists():
                self.cache_file.unlink()
            return True
        except OSError:
            return False

    def get_cache_stats(self) -> Optional[dict]:
        """Get statistics about the cached index

        Returns:
            Dictionary with cache statistics or None if no cache
        """
        if not self.cache_file.exists():
            return None

        try:
            with self.cache_file.open("rb") as f:
                cache_data = pickle.load(f)

            total_files = sum(len(loc.get("files", [])) for loc in cache_data.get("locations", {}).values())

            return {
                "timestamp": cache_data.get("timestamp", "Unknown"),
                "total_files": total_files,
                "locations": len(cache_data.get("locations", {})),
                "size": self.cache_file.stat().st_size,
            }
        except (pickle.UnpicklingError, OSError, EOFError, KeyError):
            return None
