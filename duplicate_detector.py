#!/usr/bin/env python3
"""
Duplicate Detection Module

Provides file duplicate detection capabilities with hash-based comparison
and in-memory caching for single-run performance optimization.

Features:
- Fast hash calculation with configurable algorithms
- In-memory hash cache to avoid recalculating same files
- Size-based pre-filtering for efficiency
- Streaming hash calculation for large files
- Cross-platform file comparison
"""

import hashlib
import pathlib
import sqlite3
import time
from typing import Optional

try:
    import xxhash

    XXHASH_AVAILABLE = True
except ImportError:
    XXHASH_AVAILABLE = False


class DuplicateDetector:
    """High-performance duplicate file detection with in-memory caching"""

    def __init__(self, hash_algorithm: str = "md5", chunk_size: int = 65536, tool_name: str = "duplicate_detector"):
        """Initialize duplicate detector

        Args:
            hash_algorithm: Hash algorithm to use ('md5', 'sha256', or 'xxhash64')
            chunk_size: Chunk size for streaming hash calculation
            tool_name: Name of the tool using this detector for database tracking
        """
        self.hash_algorithm = hash_algorithm.lower()
        self.chunk_size = chunk_size
        self.tool_name = tool_name

        if self.hash_algorithm == "xxhash64" and not XXHASH_AVAILABLE:
            raise ValueError("xxhash package required for xxhash64 algorithm. Install with: pip install xxhash")

        if self.hash_algorithm not in ("md5", "sha256", "xxhash64"):
            raise ValueError("Hash algorithm must be 'md5', 'sha256', or 'xxhash64'")

        # Simple in-memory cache: file_path -> hash
        self._hash_cache: dict[str, str] = {}
        self._cache_db_path = None  # Will be set by monosis if cache exists

        if hash_algorithm == "md5":
            self._hash_func = hashlib.md5
        elif hash_algorithm == "sha256":
            self._hash_func = hashlib.sha256
        else:  # xxhash64
            self._hash_func = None  # Will use xxhash.xxh64() directly

    def calculate_file_hash(self, file_path: pathlib.Path) -> str:
        """Calculate hash of a file with in-memory caching

        Args:
            file_path: Path to the file

        Returns:
            Hex digest of the file hash

        Raises:
            OSError: If file cannot be read
        """
        file_key = str(file_path)

        # Check in-memory cache first
        if file_key in self._hash_cache:
            return self._hash_cache[file_key]

        # Check database cache if available
        if self._cache_db_path and self._cache_db_path.exists():
            cached_hash = self._check_db_cache(file_path)
            if cached_hash:
                self._hash_cache[file_key] = cached_hash
                return cached_hash

        # Calculate hash
        try:
            if self.hash_algorithm == "xxhash64":
                hash_obj = xxhash.xxh64()
                with file_path.open("rb") as f:
                    while chunk := f.read(self.chunk_size):
                        hash_obj.update(chunk)
                file_hash = hash_obj.hexdigest()
            else:
                # Use standard hashlib
                hash_obj = self._hash_func()
                with file_path.open("rb") as f:
                    while chunk := f.read(self.chunk_size):
                        hash_obj.update(chunk)
                file_hash = hash_obj.hexdigest()

            # Store in memory cache
            self._hash_cache[file_key] = file_hash

            # Store in database cache if available
            if self._cache_db_path and self._cache_db_path.exists():
                self._save_to_db_cache(file_path, file_hash)

            return file_hash

        except OSError as e:
            raise OSError(f"Cannot read file {file_path}: {e}") from e

    def _check_db_cache(self, file_path: pathlib.Path) -> Optional[str]:
        """Check if file hash exists in database cache"""
        try:
            stat = file_path.stat()
            with sqlite3.connect(self._cache_db_path) as conn:
                cursor = conn.execute(
                    "SELECT full_hash FROM file_hashes WHERE file_path = ? AND file_size = ? AND mtime = ?",
                    (str(file_path), stat.st_size, stat.st_mtime),
                )
                result = cursor.fetchone()
                return result[0] if result else None
        except (OSError, sqlite3.Error):
            return None

    def _save_to_db_cache(self, file_path: pathlib.Path, file_hash: str):
        """Save file hash to database cache with tool name"""
        try:
            stat = file_path.stat()
            with sqlite3.connect(self._cache_db_path) as conn:
                # Replace existing entry for this file path
                conn.execute(
                    """
                    INSERT OR REPLACE INTO file_hashes 
                    (file_path, file_size, mtime, full_hash, hash_algorithm, tool_name, last_scan)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(file_path),
                        stat.st_size,
                        stat.st_mtime,
                        file_hash,
                        self.hash_algorithm,
                        self.tool_name,
                        time.time(),
                    ),
                )
                conn.commit()
        except (OSError, sqlite3.Error):
            # Silently ignore database errors - don't break hash calculation
            pass

    def files_are_identical(self, file1: pathlib.Path, file2: pathlib.Path) -> bool:
        """Check if two files are identical

        Args:
            file1: First file path
            file2: Second file path

        Returns:
            True if files are identical, False otherwise
        """
        try:
            # Quick size check first
            stat1 = file1.stat()
            stat2 = file2.stat()

            if stat1.st_size != stat2.st_size:
                return False

            # If same inode, they're the same file
            if stat1.st_ino == stat2.st_ino and stat1.st_dev == stat2.st_dev:
                return True

            # Hash comparison
            hash1 = self.calculate_file_hash(file1)
            hash2 = self.calculate_file_hash(file2)

            return hash1 == hash2

        except OSError:
            return False

    def find_duplicates(self, file_paths: list[pathlib.Path]) -> dict[str, list[pathlib.Path]]:
        """Find duplicate files in a list of paths

        Args:
            file_paths: List of file paths to check

        Returns:
            Dictionary mapping hash -> list of duplicate files
        """
        # Group by size first for efficiency
        size_groups: dict[int, list[pathlib.Path]] = {}

        for file_path in file_paths:
            try:
                size = file_path.stat().st_size
                if size not in size_groups:
                    size_groups[size] = []
                size_groups[size].append(file_path)
            except OSError:
                continue  # Skip files we can't stat

        # Only check files that have potential duplicates (same size)
        potential_duplicates = []
        for _size, paths in size_groups.items():
            if len(paths) > 1:
                potential_duplicates.extend(paths)

        # Hash files that could be duplicates
        hash_groups: dict[str, list[pathlib.Path]] = {}

        for file_path in potential_duplicates:
            try:
                file_hash = self.calculate_file_hash(file_path)
                if file_hash not in hash_groups:
                    hash_groups[file_hash] = []
                hash_groups[file_hash].append(file_path)
            except OSError:
                continue  # Skip files we can't read

        # Return only groups with actual duplicates
        return {hash_val: paths for hash_val, paths in hash_groups.items() if len(paths) > 1}

    def find_duplicate_files(
        self, directories: list[pathlib.Path], recursive: bool = True, extensions: Optional[set[str]] = None
    ) -> dict[str, list[pathlib.Path]]:
        """Find duplicate files in directories

        Args:
            directories: List of directories to search
            recursive: Whether to search recursively
            extensions: File extensions to include (None = all files)

        Returns:
            Dictionary mapping hash -> list of duplicate files
        """
        file_paths = []

        for directory in directories:
            if not directory.is_dir():
                continue

            pattern = "**/*" if recursive else "*"

            for file_path in directory.glob(pattern):
                if not file_path.is_file():
                    continue

                if extensions is not None:
                    ext = file_path.suffix.lower().lstrip(".")
                    if ext not in extensions:
                        continue

                file_paths.append(file_path)

        return self.find_duplicates(file_paths)

    def get_cache_stats(self) -> dict[str, int]:
        """Get in-memory cache statistics"""
        return {"cached_files": len(self._hash_cache), "algorithm": self.hash_algorithm}
