#!/usr/bin/env python3
"""
Photochronos - Photo and Video Organization Tool

Organizes photos and videos by renaming them with timestamp-based names,
removing duplicates, and optionally organizing into folder structures.

Features:
- Date-based renaming (YYYYMMDD_HHMMSS format)
- EXIF data extraction from images
- Video metadata extraction
- Duplicate detection and removal
- Interactive preview and confirmation
- Modern CLI with progress bars
"""

import argparse
import datetime
import os
import pathlib
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional

from console_ui import ConsoleUI

# Local imports
from duplicate_detector import DuplicateDetector
from file_analyzer import FFPROBE_AVAILABLE, FileAnalysisResult, FileAnalyzer
from file_operations import FileOperations, OperationType
from kosmos_config import SharedConfigManager, init_shared_cache_db

# Fix Windows console encoding for Unicode characters
if sys.platform == "win32":
    import io

    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

WINDOWS_METADATA = sys.platform == "win32"

# Configuration constants
COUNTER_FORMAT = "02d"

# Family device configuration - customize these for your family's devices
FAMILY_DEVICES = {
    # Apple devices
    "apple": ["iPhone", "iPad", "iPod"],
    # Samsung devices
    "samsung": ["Galaxy", "SM-", "GT-"],
    # Other common family camera brands
    "cameras": ["Canon", "Nikon", "Sony", "Fujifilm", "Olympus", "Panasonic"],
}

# Known messaging app signatures in EXIF software field
MESSAGING_APP_SIGNATURES = [
    "whatsapp",
    "signal",
    "telegram",
    "facebook",
    "messenger",
    "instagram",
    "snapchat",
    "twitter",
    "wechat",
    "line",
]

# Minimum EXIF tags expected in original photos
EXPECTED_EXIF_TAGS = ["Make", "Model", "DateTimeOriginal", "ExifImageWidth", "ExifImageHeight"]

# File type definitions
IMAGE_EXTENSIONS = {"jpg", "jpeg", "png", "tiff", "tif", "bmp", "gif", "webp", "srw", "raw", "cr2", "nef", "arw"}
VIDEO_EXTENSIONS = {"mp4", "mov", "avi", "mkv", "wmv", "flv", "webm", "m4v", "3gp"}
ALL_EXTENSIONS = IMAGE_EXTENSIONS | VIDEO_EXTENSIONS


@dataclass
class FileInfo:
    """Information about a media file"""

    path: pathlib.Path
    original_name: str
    file_size: int
    date_created: datetime.datetime
    file_type: str  # 'image' or 'video'
    new_name: Optional[str] = None
    target_path: Optional[pathlib.Path] = None
    file_hash: Optional[str] = None
    is_duplicate: bool = False
    duplicate_of: Optional[pathlib.Path] = None
    issues: list[str] = field(default_factory=list)
    is_external: bool = False  # True if photo is from external source (not family camera)
    external_reason: Optional[str] = None  # Reason why marked as external
    camera_make: Optional[str] = None
    camera_model: Optional[str] = None
    software: Optional[str] = None
    exif_completeness: float = 0.0  # Score 0-1 indicating EXIF data completeness


class PhotoChronos:
    """Main application class for photo/video organization"""

    def __init__(self, args):
        self.args = args
        self.files: list[FileInfo] = []
        self.duplicates: dict[str, list[FileInfo]] = defaultdict(list)
        self.issues: list[str] = []

        # Initialize duplicate detector with xxHash64 and shared cache
        # Initialize shared config and cache
        self.shared_config = SharedConfigManager()
        cache_db_path = self.shared_config.get_cache_db_path()
        init_shared_cache_db(cache_db_path)

        # Use xxHash64 for better performance (backward compatible - will rehash if needed)
        self.duplicate_detector = DuplicateDetector(
            hash_algorithm="xxhash64", chunk_size=65536, tool_name="photochronos"
        )

        # Set the shared cache path
        self.duplicate_detector._cache_db_path = cache_db_path

        # Initialize file analyzer
        self.file_analyzer = FileAnalyzer()

        # Initialize file operations
        self.file_operations = FileOperations()

        # Initialize console UI
        self.ui = ConsoleUI()

        # Extend family devices with user-provided patterns
        if args.family_devices:
            FAMILY_DEVICES["user_defined"] = args.family_devices

        # Validate inputs during initialization
        self._validate_inputs()

    def _validate_inputs(self):
        """Validate user inputs and arguments"""
        self._validate_paths()
        self._validate_extensions()
        self._validate_output_directory()

    def _validate_paths(self):
        """Validate that input paths exist and are accessible"""
        for path in self.args.path:
            if not path.exists():
                self.ui.print_error(f"Path does not exist: {path}")
                sys.exit(1)
            if not path.is_dir():
                self.ui.print_error(f"Path is not a directory: {path}")
                sys.exit(1)
            try:
                # Test read access
                list(path.iterdir())
            except PermissionError:
                self.ui.print_error(f"Permission denied accessing: {path}")
                sys.exit(1)

    def _validate_extensions(self):
        """Validate file extensions format"""
        for ext in self.args.extension:
            # Remove leading dot if present and convert to lowercase
            clean_ext = ext.lower().lstrip(".")
            if not clean_ext.isalnum():
                self.ui.print_warning(f"Extension '{ext}' contains special characters - this may cause issues")

    def _validate_output_directory(self):
        """Validate output directory if specified"""
        if self.args.output_dir:
            parent_dir = self.args.output_dir.parent
            if not parent_dir.exists():
                self.ui.print_error(f"Output directory parent does not exist: {parent_dir}")
                sys.exit(1)
            try:
                # Test write access by attempting to create the directory
                self.args.output_dir.mkdir(parents=True, exist_ok=True)
            except PermissionError:
                self.ui.print_error(f"Permission denied creating output directory: {self.args.output_dir}")
                sys.exit(1)

    def find_media_files(self) -> list[pathlib.Path]:
        """Find all media files in specified directories"""
        files = []
        extensions = {ext.lower() for ext in self.args.extension}

        self.ui.print_progress("Discovering files...")

        for path_obj in self.args.path:
            if not path_obj.exists():
                self.ui.print_warning(f"Path does not exist: {path_obj}")
                continue

            # Use pathlib for better cross-platform support
            pattern = "**/*" if self.args.recursive else "*"

            for file_path in path_obj.glob(pattern):
                if file_path.is_file() and file_path.suffix.lower().lstrip(".") in extensions:
                    files.append(file_path)

        return files

    def analyze_files(self, file_paths: list[pathlib.Path]) -> list[FileInfo]:
        """Analyze files and extract metadata using FileAnalyzer"""
        if not file_paths:
            self.ui.print_error("No files found to process")
            return []

        files = []

        # Create Rich progress bar for file analysis
        with self.ui.create_progress() as progress:
            task = progress.add_task("Analyzing files...", total=len(file_paths))

            for file_path in file_paths:
                try:
                    # Use FileAnalyzer to get metadata
                    analysis_result = self.file_analyzer.analyze_file(file_path)

                    # Convert FileAnalysisResult to FileInfo
                    file_ext = file_path.suffix.lower().lstrip(".")
                    file_type = "image" if file_ext in IMAGE_EXTENSIONS else "video"

                    file_info = FileInfo(
                        path=file_path,
                        original_name=file_path.name,
                        file_size=analysis_result.file_size,
                        date_created=analysis_result.date_created,  # Already naive from FileAnalyzer
                        file_type=file_type,
                    )

                    # Copy over metadata from analysis
                    file_info.camera_make = analysis_result.camera_make
                    file_info.camera_model = analysis_result.camera_model
                    file_info.issues.extend(analysis_result.issues)

                    # Calculate EXIF completeness for external photo detection
                    if analysis_result.has_exif and analysis_result.raw_metadata:
                        available_tags = sum(
                            1 for tag in EXPECTED_EXIF_TAGS if any(tag in str(k) for k in analysis_result.raw_metadata)
                        )
                        file_info.exif_completeness = available_tags / len(EXPECTED_EXIF_TAGS)

                    # Extract software info if available
                    if analysis_result.raw_metadata and "Image Software" in analysis_result.raw_metadata:
                        file_info.software = str(analysis_result.raw_metadata["Image Software"]).strip()

                    # Detect if photo is from external source
                    self.detect_external_photo(file_info)

                    files.append(file_info)

                except Exception as e:
                    # Create a file_info for failed analysis
                    file_info = FileInfo(
                        path=file_path,
                        original_name=file_path.name,
                        file_size=0,
                        date_created=datetime.datetime.fromtimestamp(
                            file_path.stat().st_mtime, tz=datetime.timezone.utc
                        ),
                        file_type="unknown",
                    )
                    file_info.issues.append(f"Analysis failed: {e}")
                    files.append(file_info)

                progress.update(task, advance=1)

        return files

    def detect_external_photo(self, file_info: FileInfo):
        """Detect if photo is from external source using hybrid approach"""
        # Only check images for now (videos have limited metadata)
        if file_info.file_type != "image":
            return

        # Check 1: Camera model against family device list
        if file_info.camera_make and file_info.camera_model:
            is_family_device = False

            # Check each family device category
            for _category, patterns in FAMILY_DEVICES.items():
                for pattern in patterns:
                    if (
                        pattern.lower() in file_info.camera_make.lower()
                        or pattern.lower() in file_info.camera_model.lower()
                    ):
                        is_family_device = True
                        break
                if is_family_device:
                    break

            if not is_family_device:
                file_info.is_external = True
                file_info.external_reason = f"Unknown device: {file_info.camera_make} {file_info.camera_model}"
                return

        # Check 2: Software field for messaging app signatures
        if file_info.software:
            software_lower = file_info.software.lower()
            for app in MESSAGING_APP_SIGNATURES:
                if app in software_lower:
                    file_info.is_external = True
                    file_info.external_reason = f"Messaging app detected: {app}"
                    return

        # Check 3: EXIF completeness (no camera info = likely external)
        if (
            not file_info.camera_make and not file_info.camera_model and file_info.exif_completeness < 0.4
        ):  # Less than 40% of expected tags
            file_info.is_external = True
            file_info.external_reason = "Minimal EXIF data (likely downloaded/shared)"
            return

    def show_issues_report(self, files: list[FileInfo]):
        """Show summary of issues encountered during analysis"""
        files_with_issues = [f for f in files if f.issues]

        if not files_with_issues:
            self.ui.print_info("No issues found during analysis")
            return

        # Use Rich to display issues
        issue_groups = defaultdict(list)
        for file_info in files_with_issues:
            for issue in file_info.issues:
                issue_groups[issue].append(file_info.original_name)

        self.ui.show_issues_report(issue_groups, f"Issues found with {len(files_with_issues)} files")

    def generate_new_filename(self, file_info: FileInfo) -> str:
        """Generate new filename based on date created.

        Args:
            file_info: FileInfo object containing file metadata

        Returns:
            str: New filename in format YYYYMMDD_HHMMSS.ext

        Example:
            >>> generate_new_filename(file_info)
            '20241225_143022.jpg'
        """
        date = file_info.date_created
        extension = file_info.path.suffix.lower()

        base_name = date.strftime("%Y%m%d_%H%M%S")
        return f"{base_name}{extension}"

    def generate_target_path(self, file_info: FileInfo) -> pathlib.Path:
        """Generate target path including folder organization if enabled.

        Args:
            file_info: FileInfo object containing file metadata

        Returns:
            pathlib.Path: Complete target path for the file

        Example:
            Without --organize: /photos/20241225_143022.jpg
            With --organize: /photos/2024/2024-12/20241225_143022.jpg
        """
        new_filename = file_info.new_name or self.generate_new_filename(file_info)

        if not self.args.organize:
            # Use output directory if specified, otherwise keep in same directory
            if self.args.output_dir:
                return self.args.output_dir / new_filename
            return file_info.path.parent / new_filename

        # Organize into year/year-month structure (e.g., 2024/2024-12)
        date = file_info.date_created
        year = date.strftime("%Y")
        year_month = date.strftime("%Y-%m")

        # Add "extern" suffix for external photos
        if file_info.is_external:
            year_month = f"{year_month} extern"

        # Determine base directory
        if self.args.output_dir:
            base_dir = self.args.output_dir
        else:
            # Use the directory of the source file as base
            base_dir = file_info.path.parent

        target_dir = base_dir / year / year_month
        return target_dir / new_filename

    def _increment_filename(self, base_name: str, counter: int) -> str:
        """Generate incremented filename with counter suffix"""
        name_part = base_name.rsplit(".", 1)[0]
        ext_part = base_name.rsplit(".", 1)[1] if "." in base_name else ""
        counter_str = f"{counter:{COUNTER_FORMAT}}"
        return f"{name_part}_{counter_str}.{ext_part}" if ext_part else f"{name_part}_{counter_str}"

    def _resolve_naming_conflicts(
        self, file_info: FileInfo, base_new_name: str, used_target_paths: set[str]
    ) -> tuple[str, pathlib.Path]:
        """Resolve naming conflicts and return final name and target path"""
        new_name = base_new_name
        counter = 1

        while True:
            file_info.new_name = new_name
            target_path = self.generate_target_path(file_info)
            target_path_str = str(target_path)

            # Check if already used in current batch
            if target_path_str in used_target_paths:
                if target_path.exists():
                    # Check batch duplicate flag first (cheap)
                    if self._is_already_duplicate(file_info):
                        return new_name, pathlib.Path()  # Batch duplicate
                    # Compare content against existing target file
                    if self._check_identical_to_target(file_info, target_path):
                        return new_name, pathlib.Path()  # Already at target
                new_name = self._increment_filename(base_new_name, counter)
                counter += 1
                continue

            # Check if target file already exists on disk
            if target_path.exists():
                # Check if this is the same file (already in correct location)
                if file_info.path.resolve() == target_path.resolve():
                    # Same file, already in correct location - no processing needed
                    return new_name, target_path

                # Check batch duplicate flag first (cheap)
                if self._is_already_duplicate(file_info):
                    return new_name, pathlib.Path()  # Batch duplicate

                # Compare content against existing target file
                if self._check_identical_to_target(file_info, target_path):
                    return new_name, pathlib.Path()  # Already at target

                # Different file with same name - increment and try again
                new_name = self._increment_filename(base_new_name, counter)
                counter += 1
                continue

            # No conflicts - we can use this target path
            return new_name, target_path

    def _detect_content_duplicates(self, files: list[FileInfo]):
        """Detect content duplicates efficiently by grouping files with same target names.

        Strategy: group by target filename (O(n)), then within each group hash each file
        once and group by hash (O(k) per group) to find duplicates without pairwise comparison.
        """
        # Group files by their target filename - O(n)
        target_name_groups = defaultdict(list)
        for file_info in files:
            target_name = self.generate_new_filename(file_info)
            target_name_groups[target_name].append(file_info)

        # Within each group that has potential conflicts, hash and group - O(k) per group
        for _target_name, file_group in target_name_groups.items():
            if len(file_group) <= 1:
                continue

            # Hash each file once and group by hash
            hash_groups: dict[str, list[FileInfo]] = defaultdict(list)
            for file_info in file_group:
                try:
                    file_hash = self.duplicate_detector.calculate_file_hash(file_info.path)
                    hash_groups[file_hash].append(file_info)
                except Exception as e:
                    file_info.issues.append(f"Duplicate check failed: {e}")

            # Within each hash group, keep the first (by path order), mark rest as duplicates
            for _hash_val, identical_files in hash_groups.items():
                if len(identical_files) <= 1:
                    continue
                identical_files.sort(key=lambda f: str(f.path))
                original = identical_files[0]
                for duplicate in identical_files[1:]:
                    duplicate.is_duplicate = True
                    duplicate.duplicate_of = original.path

    def _is_already_duplicate(self, file_info: FileInfo) -> bool:
        """Check if file was already marked as duplicate by _detect_content_duplicates."""
        return file_info.is_duplicate

    def _check_identical_to_target(self, file_info: FileInfo, target_path: pathlib.Path) -> bool:
        """Compare source file content against an existing target file.
        If identical, mark source as duplicate of the target for cleanup."""
        try:
            if self.duplicate_detector.files_are_identical(file_info.path, target_path):
                file_info.is_duplicate = True
                file_info.duplicate_of = target_path
                return True
        except Exception as e:
            file_info.issues.append(f"Target comparison failed: {e}")
        return False

    def plan_renames(self, files: list[FileInfo]) -> dict[str, FileInfo]:
        """Plan renames with conflict resolution"""
        if not files:
            return {}

        self.ui.print_progress("Planning renames and organization...")

        # Sort files by date for consistent processing
        sorted_files = sorted(files, key=lambda f: f.date_created)

        # First pass: Detect duplicates by content across all files
        self._detect_content_duplicates(sorted_files)

        # Track used target paths to handle conflicts
        used_target_paths: set[str] = set()
        planned_operations: dict[str, FileInfo] = {}

        # Create Rich progress bar for planning operations
        with self.ui.create_progress() as progress:
            task = progress.add_task("Planning operations...", total=len(sorted_files))

            for file_info in sorted_files:
                # Skip duplicates that were already detected in the first pass
                if file_info.is_duplicate:
                    progress.update(task, advance=1)
                    continue

                base_new_name = self.generate_new_filename(file_info)

                # Resolve naming conflicts and get final target path
                final_name, target_path = self._resolve_naming_conflicts(file_info, base_new_name, used_target_paths)

                # Skip processing if it's a duplicate (empty path returned)
                if str(target_path) == "." or target_path == pathlib.Path():
                    progress.update(task, advance=1)
                    continue

                used_target_paths.add(str(target_path))
                file_info.new_name = final_name
                file_info.target_path = target_path

                # Plan operation if file needs to move/rename and isn't a duplicate
                current_path_str = str(file_info.path)
                target_path_str = str(target_path)

                # Only add operation if there's an actual change needed
                if (
                    current_path_str != target_path_str
                    and not file_info.is_duplicate
                    and (file_info.path.name != target_path.name or file_info.path.parent != target_path.parent)
                ):
                    planned_operations[current_path_str] = file_info

                progress.update(task, advance=1)

        return planned_operations

    def prompt_duplicate_deletion(self, files: list[FileInfo]) -> bool:
        """Prompt user to confirm duplicate deletion"""
        duplicates = [f for f in files if f.is_duplicate]

        if not duplicates:
            return False

        print()  # Empty line before prompt
        self.ui.print_warning(f"Found {len(duplicates)} duplicate files that can be safely deleted.")

        try:
            response = input(f"\nDelete these {len(duplicates)} duplicate files? [y/N]: ").strip().lower()
            return response in ["y", "yes"]
        except (EOFError, KeyboardInterrupt):
            return False

    def delete_duplicates(self, files: list[FileInfo]) -> tuple[int, int]:
        """Delete duplicate files and return (success_count, error_count)"""
        duplicates = [f for f in files if f.is_duplicate]

        if not duplicates:
            return 0, 0

        success_count = 0
        error_count = 0

        # Create Rich progress bar for deletion
        with self.ui.create_progress() as progress:
            task = progress.add_task("Deleting duplicates...", total=len(duplicates))

            for file_info in duplicates:
                try:
                    file_info.path.unlink()  # Delete the file
                    success_count += 1
                except Exception as e:
                    self.ui.print_error(f"Failed to delete {file_info.path.name}: {e}")
                    error_count += 1

                progress.update(task, advance=1)

        return success_count, error_count

    def count_unnecessary_suffixes(self, planned_operations: dict[str, FileInfo]) -> int:
        """Count files that got unnecessary _XX suffixes due to naming conflicts"""
        if not planned_operations:
            return 0

        # Get all target filenames that will exist after the operations
        final_target_names = set()
        for file_info in planned_operations.values():
            if file_info.target_path:
                final_target_names.add(file_info.target_path.name)

        # Count files with suffixes that could use the unsuffixed name
        unnecessary_suffixes = 0
        for file_info in planned_operations.values():
            if file_info.target_path and "_" in file_info.target_path.name:
                # Check if this filename has a suffix like _01, _02, etc.
                name_parts = file_info.target_path.stem.split("_")
                if len(name_parts) >= 2 and name_parts[-1].isdigit() and len(name_parts[-1]) == 2:
                    # This file has a numeric suffix
                    # Check if the unsuffixed version would be available in the final state
                    base_name = "_".join(name_parts[:-1]) + file_info.target_path.suffix
                    if base_name not in final_target_names:
                        unnecessary_suffixes += 1

        return unnecessary_suffixes

    def show_duplicates(self, files: list[FileInfo]):
        """Show files that are duplicates and can be safely deleted"""
        duplicates = [f for f in files if f.is_duplicate]

        if not duplicates:
            return

        print()  # Empty line before duplicates
        self.ui.print_warning(f"Found {len(duplicates)} duplicate files (can be safely deleted):")

        # Group by directory for cleaner display
        by_directory = defaultdict(list)
        for file_info in duplicates:
            by_directory[str(file_info.path.parent)].append(file_info)

        for directory, dir_files in sorted(by_directory.items()):
            self.ui.print_warning(f"  Directory: {directory}")
            for file_info in dir_files:
                self.ui.console.print(
                    f"    {file_info.original_name} (duplicate of {file_info.duplicate_of.name})", style="yellow dim"
                )

        self.ui.print_info(
            f"  These {len(duplicates)} files are identical to existing target files and can be safely deleted."
        )
        print()  # Empty line after duplicates

    def interactive_device_selection(self, files: list[FileInfo]):
        """Allow user to interactively select family devices from detected cameras"""
        # Collect all unique camera devices found
        devices_found = {}
        for file_info in files:
            if file_info.camera_make and file_info.camera_model:
                device_key = f"{file_info.camera_make} {file_info.camera_model}".strip()
                if device_key not in devices_found:
                    devices_found[device_key] = 0
                devices_found[device_key] += 1

        if not devices_found:
            return  # No devices to select from

        # Sort by count (most common first)
        sorted_devices = sorted(devices_found.items(), key=lambda x: x[1], reverse=True)

        print()  # Empty line
        self.ui.console.print("Camera devices found in photos:")
        self.ui.print_info("Photos from family devices go to regular folders, others go to 'extern' folders")
        print()

        # Display devices with numbers
        for i, (device, count) in enumerate(sorted_devices, 1):
            self.ui.console.print(f"  [{i}] {device} ({count} photos)")

        print()
        self.ui.console.print("Select family devices by entering numbers (e.g., '1 3 5') or press Enter to skip:")

        try:
            response = input("> ").strip()
            if response:
                # Parse selected numbers
                selected_indices = []
                for num_str in response.split():
                    try:
                        num = int(num_str)
                        if 1 <= num <= len(sorted_devices):
                            selected_indices.append(num - 1)
                    except ValueError:
                        continue

                if selected_indices:
                    # Add selected devices to family devices
                    selected_devices = [sorted_devices[i][0] for i in selected_indices]
                    FAMILY_DEVICES["user_selected"] = selected_devices

                    # Re-run external photo detection with new devices
                    self.ui.print_success(f"\nAdded {len(selected_devices)} device(s) as family devices")

                    # Re-detect external photos
                    for file_info in files:
                        file_info.is_external = False
                        file_info.external_reason = None
                        self.detect_external_photo(file_info)

        except KeyboardInterrupt:
            print("\nDevice selection cancelled")

    def show_external_photos_report(self, files: list[FileInfo]):
        """Show summary of external photos detected"""
        external_photos = [f for f in files if f.is_external]

        if not external_photos:
            return  # Don't show anything if no external photos

        print()  # Empty line before report
        self.ui.console.print(f"External photos detected: {len(external_photos)} files")

        # Group by reason for cleaner display
        reason_groups = defaultdict(list)
        for file_info in external_photos:
            reason = file_info.external_reason or "Unknown reason"
            reason_groups[reason].append(file_info.original_name)

        for reason, filenames in sorted(reason_groups.items()):
            self.ui.console.print(f"  {reason} ({len(filenames)} files):")
            # Show first few filenames
            show_count = min(3, len(filenames))
            for filename in filenames[:show_count]:
                self.ui.console.print(f"    - {filename}", style="white dim")
            if len(filenames) > show_count:
                self.ui.console.print(f"    - ... and {len(filenames) - show_count} more", style="white dim")

        if self.args.organize:
            self.ui.print_info("  These files will be organized into 'extern' folders")
        print()  # Empty line after report

    def show_analysis_summary(self, files: list[FileInfo], duplicates_count: int):
        """Show brief summary of issues and duplicates"""
        files_with_issues = [f for f in files if f.issues]

        if not files_with_issues and duplicates_count == 0:
            return

        print()  # Empty line before summary

        if files_with_issues:
            self.ui.print_warning(f"{len(files_with_issues)} files had analysis issues (see details above)")

        if duplicates_count > 0:
            self.ui.print_warning(f"{duplicates_count} duplicate files found (see details above)")

    def prompt_operations_confirmation(self, planned_operations: dict[str, FileInfo]) -> bool:
        """Confirm planned operations before execution"""
        if not planned_operations:
            return True

        print()  # Empty line before prompt
        self.ui.console.print(f"{len(planned_operations)} files ready to process")

        try:
            response = input("\nProceed with these operations? [y/N]: ").strip().lower()
            return response in ["y", "yes"]
        except KeyboardInterrupt:
            print("\nOperation cancelled by user")
            return False

    def execute_operations(self, planned_operations: dict[str, FileInfo]) -> bool:
        """Execute the planned file operations using FileOperations module"""
        if not planned_operations:
            return True

        operation_verb = "copy" if self.args.copy else "move"
        operation_type = OperationType.COPY if self.args.copy else OperationType.MOVE

        self.ui.print_progress(f"{operation_verb.capitalize()}ing files to their destinations...")

        # Convert FileInfo objects to FileOperation objects
        file_mappings = {file_info.path: file_info.target_path for file_info in planned_operations.values()}
        operations = self.file_operations.plan_batch_operations(file_mappings, operation_type)

        # Execute operations with Rich progress bar
        with self.ui.create_progress() as progress:
            task = progress.add_task("Processing files...", total=len(operations))

            def progress_update(_message):
                progress.update(task, advance=1)

            self.file_operations.progress_callback = progress_update
            successful_results, failed_results = self.file_operations.execute_batch_operations(operations)

        # Convert results back to the expected format
        success_files = [result.operation.identifier for result in successful_results]
        failed_files = [(result.operation.identifier, result.error_message) for result in failed_results]

        # Show operation summary
        self.show_operation_summary(success_files, failed_files, operation_verb.lower())

        return len(failed_results) == 0

    def show_operation_summary(
        self, success_files: list[str], failed_files: list[tuple[str, str]], operation_verb: str
    ):
        """Show summary of file operations using Rich"""
        past_tense = "copied" if self.args.copy else "moved"
        self.ui.show_operation_summary(success_files, failed_files, past_tense)

    def show_rename_preview(self, planned_operations: dict[str, FileInfo]):
        """Show preview of planned operations grouped by target directory"""
        if not planned_operations:
            self.ui.console.print("No files need processing")
            return

        operation_type = "Organization" if self.args.organize else "Rename"
        self.ui.console.print(f"\n{operation_type} preview ({len(planned_operations)} files):")

        # Group by target directory for cleaner display
        by_target_directory = defaultdict(list)
        for file_info in planned_operations.values():
            target_dir = str(file_info.target_path.parent)
            by_target_directory[target_dir].append(file_info)

        for target_dir, files in sorted(by_target_directory.items()):
            self.ui.console.print(f"\nTarget: {target_dir}")
            for file_info in files:
                source = f"{file_info.path.parent.name}/{file_info.original_name}"
                target = file_info.target_path.name

                if self.args.organize:
                    # Show full path change for organization
                    self.ui.console.print(f"  {source} → {target}", style="white dim")
                else:
                    # Show just rename
                    self.ui.console.print(f"  {file_info.original_name} → {target}", style="white dim")

    def show_configuration(self):
        """Show current configuration using Rich"""
        config = {
            "Paths": ", ".join(str(p) for p in self.args.path),
            "Recursive": "Yes" if self.args.recursive else "No",
            "Extensions": ", ".join(sorted(self.args.extension)),
            "Dry run": "Yes" if self.args.dry_run else "No",
            "Organize into folders": "Yes" if self.args.organize else "No",
            "Operation mode": "Copy" if self.args.copy else "Move",
        }

        if self.args.output_dir:
            config["Output directory"] = str(self.args.output_dir)

        if self.args.organize:
            config["External photo detection"] = "Enabled"

        if self.args.family_devices:
            config["Additional family devices"] = ", ".join(self.args.family_devices)

        if FFPROBE_AVAILABLE:
            config["Video metadata"] = "Full (ffprobe)"
        elif WINDOWS_METADATA:
            config["Video metadata"] = "Full (Windows COM)"
        else:
            config["Video metadata"] = "Limited (no ffprobe or Windows COM)"

        # Show cache stats if there are cached hashes
        cache_stats = self.duplicate_detector.get_cache_stats()
        if cache_stats["cached_files"] > 0:
            config["Hash cache"] = f"{cache_stats['cached_files']} files ({cache_stats['algorithm']})"

        self.ui.show_configuration(config, "PhotoChronos Configuration")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="PhotoChronos - Organize photos and videos by date",
        epilog="Renames media files to YYYYMMDD_HHMMSS format based on creation date",
    )

    parser.add_argument("path", type=pathlib.Path, nargs="+", help="Directories to process")

    parser.add_argument(
        "-e",
        "--extension",
        nargs="+",
        default=list(ALL_EXTENSIONS),
        help=f"File extensions to process (default: {', '.join(sorted(ALL_EXTENSIONS))})",
    )

    parser.add_argument("-r", "--recursive", action="store_true", help="Process subdirectories recursively")

    parser.add_argument("-d", "--dry-run", action="store_true", help="Show what would be done without making changes")

    parser.add_argument("--organize", action="store_true", help="Organize files into year/year-month folder structure")

    parser.add_argument(
        "-o", "--output-dir", type=pathlib.Path, help="Base directory for organized output (default: same as source)"
    )

    parser.add_argument(
        "--copy", action="store_true", help="Copy files instead of moving them (leaves originals intact)"
    )

    parser.add_argument(
        "--family-devices", nargs="+", help='Additional family device patterns to recognize (e.g., "Pixel 7" "OnePlus")'
    )

    parser.add_argument(
        "--delete-duplicates",
        action="store_true",
        help="Delete duplicate files (files identical to others being processed)",
    )

    args = parser.parse_args()

    # Initialize PhotoChronos
    app = PhotoChronos(args)

    # Show configuration
    app.show_configuration()

    # Find and analyze files
    file_paths = app.find_media_files()

    if not file_paths:
        app.ui.console.print("No media files found in specified directories")
        return 0

    app.ui.print_success(f"Found {len(file_paths)} media files to process")

    # Analyze files for metadata
    files = app.analyze_files(file_paths)

    if not files:
        app.ui.print_error("No files could be analyzed")
        return 1

    app.ui.print_success(f"Successfully analyzed {len(files)} files")

    # First step: Show issues and reports
    app.show_issues_report(files)

    # Interactive device selection if organizing and no custom devices provided
    if args.organize and not args.family_devices:
        app.interactive_device_selection(files)

    app.show_external_photos_report(files)

    # Plan operations (renames and/or organization) - This detects duplicates
    planned_operations = app.plan_renames(files)

    # Now show duplicates that were found during planning
    app.show_duplicates(files)

    # Delete duplicates if requested
    deleted_success = 0
    deleted_errors = 0
    if args.delete_duplicates:
        if args.dry_run:
            duplicates_count = sum(1 for f in files if f.is_duplicate)
            if duplicates_count > 0:
                app.ui.print_info(f"Would delete {duplicates_count} duplicate files (dry run mode)")
        # Prompt user for confirmation before deleting
        elif app.prompt_duplicate_deletion(files):
            deleted_success, deleted_errors = app.delete_duplicates(files)
            # Mark successfully deleted files as no longer duplicates for summary
            if deleted_success > 0:
                for file_info in files:
                    if file_info.is_duplicate and not file_info.path.exists():
                        file_info.is_duplicate = False
        else:
            app.ui.print_info("Duplicate deletion cancelled by user")

    # Count duplicates for summary (after potential deletion)
    duplicates_count = sum(1 for f in files if f.is_duplicate)

    # Show brief summary of issues
    app.show_analysis_summary(files, duplicates_count)

    # Show operation preview
    app.show_rename_preview(planned_operations)

    # Check for unnecessary suffixes and warn if found
    unnecessary_suffixes = app.count_unnecessary_suffixes(planned_operations)
    if unnecessary_suffixes > 0:
        print()  # Empty line before warning
        app.ui.print_warning("Naming conflicts detected!")
        app.ui.console.print(
            f"{unnecessary_suffixes} files received unnecessary _01 suffixes due to timestamp conflicts.",
            style="yellow dim",
        )
        app.ui.console.print(
            "Recommendation: Run the command again after this operation completes.", style="yellow dim"
        )
        app.ui.console.print("The second run will clean up these naming conflicts.", style="yellow dim")

    if args.dry_run:
        app.ui.print_info("\nDry run mode - no files were modified")
        return 0

    if not planned_operations and duplicates_count == 0:
        app.ui.print_success("All files are already in correct locations with correct names")
        return 0

    # Get user confirmation before proceeding
    if not app.prompt_operations_confirmation(planned_operations):
        app.ui.print_info("Operation cancelled by user")
        return 0

    # Execute the operations
    success = app.execute_operations(planned_operations)

    if success:
        app.ui.print_success("File organization completed successfully!")

        # Show deletion summary
        if deleted_success > 0:
            app.ui.print_success(f"Deleted {deleted_success} duplicate files")
        if deleted_errors > 0:
            app.ui.print_error(f"Failed to delete {deleted_errors} duplicate files")

        # Remind about remaining duplicates
        if duplicates_count > 0 and not args.delete_duplicates:
            app.ui.console.print(f"\nRemember to manually clean up the {duplicates_count} duplicate files shown above")
            app.ui.console.print("Or use --delete-duplicates to remove them automatically", style="dim")

        return 0
    app.ui.print_error("Some operations failed - check error messages above")
    return 1


if __name__ == "__main__":
    sys.exit(main())
