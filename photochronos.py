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
import pathlib
import sys
import os
import datetime
from typing import List, Dict, Tuple, Optional, Set
from collections import OrderedDict, defaultdict
from dataclasses import dataclass

# Third-party imports
from tqdm import tqdm
import exifread
from tzlocal import get_localzone
import colorama
from colorama import Fore, Style, Back
import shutil

# Local imports
from duplicate_detector import DuplicateDetector

# Initialize colorama for cross-platform color support
colorama.init()

# Fix Windows console encoding for Unicode characters
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Windows-specific imports for video metadata
try:
    from win32com.propsys import propsys, pscon
    WINDOWS_METADATA = True
except ImportError:
    WINDOWS_METADATA = False

# Configuration constants
COUNTER_FORMAT = "02d"

# Family device configuration - customize these for your family's devices
FAMILY_DEVICES = {
    # Apple devices
    'apple': ['iPhone', 'iPad', 'iPod'],
    # Samsung devices
    'samsung': ['Galaxy', 'SM-', 'GT-'],
    # Other common family camera brands
    'cameras': ['Canon', 'Nikon', 'Sony', 'Fujifilm', 'Olympus', 'Panasonic']
}

# Known messaging app signatures in EXIF software field
MESSAGING_APP_SIGNATURES = [
    'whatsapp', 'signal', 'telegram', 'facebook', 'messenger',
    'instagram', 'snapchat', 'twitter', 'wechat', 'line'
]

# Minimum EXIF tags expected in original photos
EXPECTED_EXIF_TAGS = [
    'Make', 'Model', 'DateTimeOriginal', 'ExifImageWidth', 'ExifImageHeight'
]

# File type definitions
IMAGE_EXTENSIONS = {'jpg', 'jpeg', 'png', 'tiff', 'tif', 'bmp', 'gif', 'webp', 'srw', 'raw', 'cr2', 'nef', 'arw'}
VIDEO_EXTENSIONS = {'mp4', 'mov', 'avi', 'mkv', 'wmv', 'flv', 'webm', 'm4v', '3gp'}
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
    issues: List[str] = None
    is_external: bool = False  # True if photo is from external source (not family camera)
    external_reason: Optional[str] = None  # Reason why marked as external
    camera_make: Optional[str] = None
    camera_model: Optional[str] = None
    software: Optional[str] = None
    exif_completeness: float = 0.0  # Score 0-1 indicating EXIF data completeness
    
    def __post_init__(self):
        if self.issues is None:
            self.issues = []

class PhotoChronos:
    """Main application class for photo/video organization"""
    
    def __init__(self, args):
        self.args = args
        self.files: List[FileInfo] = []
        self.duplicates: Dict[str, List[FileInfo]] = defaultdict(list)
        self.issues: List[str] = []
        
        # Initialize duplicate detector with in-memory caching
        self.duplicate_detector = DuplicateDetector(
            hash_algorithm='md5',
            chunk_size=65536
        )
        
        # Extend family devices with user-provided patterns
        if args.family_devices:
            FAMILY_DEVICES['user_defined'] = args.family_devices
        
        # Validate inputs during initialization
        self._validate_inputs()
        
    def print_result(self, message: str):
        """Print primary result/output in white"""
        print(f"{Fore.WHITE}{message}{Style.RESET_ALL}")
    
    def print_config(self, message: str):
        """Print configuration info in light cyan"""
        print(f"{Fore.CYAN}{message}{Style.RESET_ALL}")
    
    def print_info(self, message: str):
        """Print neutral information in dim white (gray)"""
        print(f"{Fore.WHITE}{Style.DIM}{message}{Style.RESET_ALL}")
    
    def print_tip(self, message: str):
        """Print tips and hints in magenta"""
        print(f"{Fore.MAGENTA}{message}{Style.RESET_ALL}")
    
    def print_progress(self, message: str):
        """Print intermediate/progress messages in dim white"""
        print(f"{Fore.WHITE}{Style.DIM}{message}{Style.RESET_ALL}")
    
    def print_success(self, message: str):
        """Print success messages in green"""
        print(f"{Fore.GREEN}{message}{Style.RESET_ALL}")
    
    def print_warning(self, message: str):
        """Print warning messages in strong yellow"""
        print(f"{Fore.YELLOW}{message}{Style.RESET_ALL}")
    
    def print_error(self, message: str):
        """Print error messages in red"""
        print(f"{Fore.RED}{message}{Style.RESET_ALL}")
    
    def _validate_inputs(self):
        """Validate user inputs and arguments"""
        self._validate_paths()
        self._validate_extensions()
        self._validate_output_directory()
    
    def _validate_paths(self):
        """Validate that input paths exist and are accessible"""
        for path in self.args.path:
            if not path.exists():
                self.print_error(f"Path does not exist: {path}")
                sys.exit(1)
            if not path.is_dir():
                self.print_error(f"Path is not a directory: {path}")
                sys.exit(1)
            try:
                # Test read access
                list(path.iterdir())
            except PermissionError:
                self.print_error(f"Permission denied accessing: {path}")
                sys.exit(1)
    
    def _validate_extensions(self):
        """Validate file extensions format"""
        for ext in self.args.extension:
            # Remove leading dot if present and convert to lowercase
            clean_ext = ext.lower().lstrip('.')
            if not clean_ext.isalnum():
                self.print_warning(f"Extension '{ext}' contains special characters - this may cause issues")
    
    def _validate_output_directory(self):
        """Validate output directory if specified"""
        if self.args.output_dir:
            parent_dir = self.args.output_dir.parent
            if not parent_dir.exists():
                self.print_error(f"Output directory parent does not exist: {parent_dir}")
                sys.exit(1)
            try:
                # Test write access by attempting to create the directory
                self.args.output_dir.mkdir(parents=True, exist_ok=True)
            except PermissionError:
                self.print_error(f"Permission denied creating output directory: {self.args.output_dir}")
                sys.exit(1)

    def find_media_files(self) -> List[pathlib.Path]:
        """Find all media files in specified directories"""
        files = []
        extensions = set(ext.lower() for ext in self.args.extension)
        
        self.print_progress("Discovering files...")
        
        for path_obj in self.args.path:
            if not path_obj.exists():
                self.print_warning(f"Path does not exist: {path_obj}")
                continue
                
            # Use pathlib for better cross-platform support
            pattern = "**/*" if self.args.recursive else "*"
            
            for file_path in path_obj.glob(pattern):
                if file_path.is_file() and file_path.suffix.lower().lstrip('.') in extensions:
                    files.append(file_path)
        
        return files

    def extract_date_from_image(self, file_path: pathlib.Path, file_info: FileInfo) -> Optional[datetime.datetime]:
        """Extract creation date and camera info from image EXIF data"""
        date_tags = [
            'EXIF DateTimeOriginal', 'DateTimeOriginal',
            'EXIF DateTimeDigitized', 'DateTimeDigitized', 
            'EXIF DateTime', 'DateTime'
        ]
        
        try:
            with open(file_path, 'rb') as file:
                tags = exifread.process_file(file, details=False)
                
                # Extract camera make and model
                if 'Image Make' in tags:
                    file_info.camera_make = str(tags['Image Make']).strip()
                if 'Image Model' in tags:
                    file_info.camera_model = str(tags['Image Model']).strip()
                if 'Image Software' in tags:
                    file_info.software = str(tags['Image Software']).strip()
                
                # Count available EXIF tags for completeness check
                available_tags = sum(1 for tag in EXPECTED_EXIF_TAGS if any(tag in str(k) for k in tags.keys()))
                file_info.exif_completeness = available_tags / len(EXPECTED_EXIF_TAGS)
                
                # Extract date
                for tag in date_tags:
                    if tag in tags:
                        date_str = str(tags[tag])
                        try:
                            return datetime.datetime.strptime(date_str, '%Y:%m:%d %H:%M:%S')
                        except ValueError:
                            continue
                            
        except Exception as e:
            file_info.issues.append(f"Could not read EXIF data: {e}")
        
        if not file_info.issues:
            file_info.issues.append("No EXIF date found")
        
        return None

    def extract_date_from_video(self, file_path: pathlib.Path, file_info: FileInfo) -> Optional[datetime.datetime]:
        """Extract creation date from video metadata (Windows only)"""
        if not WINDOWS_METADATA:
            file_info.issues.append("Video metadata not available (Windows COM required)")
            return None
            
        try:
            properties = propsys.SHGetPropertyStoreFromParsingName(str(file_path.absolute()))
            date_created = properties.GetValue(pscon.PKEY_Media_DateEncoded).GetValue()
            
            if isinstance(date_created, datetime.datetime):
                # Convert to local timezone and make naive for consistency
                local_date = date_created.astimezone(get_localzone())
                return local_date.replace(tzinfo=None)
                
        except Exception as e:
            file_info.issues.append(f"Could not read video metadata: {e}")
        
        if not file_info.issues:
            file_info.issues.append("No video metadata found")
        
        return None

    def get_file_date(self, file_path: pathlib.Path, file_info: FileInfo) -> datetime.datetime:
        """Get the creation date of a file, trying multiple methods"""
        file_ext = file_path.suffix.lower().lstrip('.')
        
        # Try EXIF for images
        if file_ext in IMAGE_EXTENSIONS:
            exif_date = self.extract_date_from_image(file_path, file_info)
            if exif_date:
                return exif_date
        
        # Try video metadata for videos
        elif file_ext in VIDEO_EXTENSIONS:
            video_date = self.extract_date_from_video(file_path, file_info)
            if video_date:
                return video_date
        
        # Fallback to file modification time
        file_info.issues.append("Using file modification time (no metadata found)")
        return datetime.datetime.fromtimestamp(file_path.stat().st_mtime)

    def analyze_files(self, file_paths: List[pathlib.Path]) -> List[FileInfo]:
        """Analyze files and extract metadata"""
        if not file_paths:
            self.print_error("No files found to process")
            return []
        
        files = []
        pbar = tqdm(total=len(file_paths), desc="Analyzing files", unit="files", leave=True)
        
        for file_path in file_paths:
            try:
                file_ext = file_path.suffix.lower().lstrip('.')
                file_type = 'image' if file_ext in IMAGE_EXTENSIONS else 'video'
                
                # Create file_info first, then get date (which may add issues)
                file_info = FileInfo(
                    path=file_path,
                    original_name=file_path.name,
                    file_size=file_path.stat().st_size,
                    date_created=datetime.datetime.now(),  # Temporary, will be updated
                    file_type=file_type
                )
                
                # Get actual date (this may add issues to file_info)
                file_info.date_created = self.get_file_date(file_path, file_info)
                
                # Detect if photo is from external source
                self.detect_external_photo(file_info)
                
                files.append(file_info)
                
            except Exception as e:
                # Create a file_info for failed analysis too
                file_info = FileInfo(
                    path=file_path,
                    original_name=file_path.name,
                    file_size=0,
                    date_created=datetime.datetime.fromtimestamp(file_path.stat().st_mtime),
                    file_type='unknown'
                )
                file_info.issues.append(f"Analysis failed: {e}")
                files.append(file_info)
            
            pbar.update(1)
        
        pbar.close()
        return files
    
    def detect_external_photo(self, file_info: FileInfo):
        """Detect if photo is from external source using hybrid approach"""
        # Only check images for now (videos have limited metadata)
        if file_info.file_type != 'image':
            return
        
        # Check 1: Camera model against family device list
        if file_info.camera_make and file_info.camera_model:
            is_family_device = False
            
            # Check each family device category
            for category, patterns in FAMILY_DEVICES.items():
                for pattern in patterns:
                    if (pattern.lower() in file_info.camera_make.lower() or 
                        pattern.lower() in file_info.camera_model.lower()):
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
        if not file_info.camera_make and not file_info.camera_model:
            if file_info.exif_completeness < 0.4:  # Less than 40% of expected tags
                file_info.is_external = True
                file_info.external_reason = "Minimal EXIF data (likely downloaded/shared)"
                return
    
    def show_issues_report(self, files: List[FileInfo]):
        """Show summary of issues encountered during analysis"""
        files_with_issues = [f for f in files if f.issues]
        
        if not files_with_issues:
            self.print_info("No issues found during analysis")
            return
        
        print()  # Empty line before issues
        # Use orange for issue headers
        print(f"{Fore.LIGHTRED_EX}Issues found with {len(files_with_issues)} files:{Style.RESET_ALL}")
        
        # Group issues by type for cleaner display
        issue_groups = defaultdict(list)
        for file_info in files_with_issues:
            for issue in file_info.issues:
                issue_groups[issue].append(file_info.original_name)
        
        for issue, filenames in issue_groups.items():
            print(f"{Fore.LIGHTRED_EX}  {issue} ({len(filenames)} files):{Style.RESET_ALL}")
            # Show first few filenames, then "and X more" if too many
            show_count = min(3, len(filenames))
            for filename in filenames[:show_count]:
                print(f"{Fore.LIGHTRED_EX}{Style.DIM}    - {filename}{Style.RESET_ALL}")
            if len(filenames) > show_count:
                print(f"{Fore.LIGHTRED_EX}{Style.DIM}    - ... and {len(filenames) - show_count} more{Style.RESET_ALL}")
    
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
        
        # Format: YYYYMMDD_HHMMSS.ext
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
            else:
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
        name_part = base_name.rsplit('.', 1)[0]
        ext_part = base_name.rsplit('.', 1)[1] if '.' in base_name else ''
        counter_str = f"{counter:{COUNTER_FORMAT}}"
        return f"{name_part}_{counter_str}.{ext_part}" if ext_part else f"{name_part}_{counter_str}"
    
    
    def _resolve_naming_conflicts(self, file_info: FileInfo, base_new_name: str, used_target_paths: Set[str]) -> tuple[str, pathlib.Path]:
        """Resolve naming conflicts and return final name and target path"""
        new_name = base_new_name
        counter = 1
        
        while True:
            file_info.new_name = new_name
            target_path = self.generate_target_path(file_info)
            target_path_str = str(target_path)
            
            # Check if already used in current batch
            if target_path_str in used_target_paths:
                # Check if this could be a duplicate by looking for existing file with same name
                # We need to check if the file might already exist on disk even if not in batch yet
                if target_path.exists():
                    duplicate_result = self._check_for_duplicate(file_info, target_path)
                    if duplicate_result:
                        return new_name, pathlib.Path()  # It's a duplicate
                new_name = self._increment_filename(base_new_name, counter)
                counter += 1
                continue
            
            # Check if target file already exists on disk
            if target_path.exists():
                # Check if this is the same file (already in correct location)
                if file_info.path.resolve() == target_path.resolve():
                    # Same file, already in correct location - no processing needed
                    return new_name, target_path
                
                # Different source file - check if it's a duplicate by content
                duplicate_result = self._check_for_duplicate(file_info, target_path)
                if duplicate_result:
                    # For duplicates, return empty path to signal no move needed
                    return new_name, pathlib.Path()  # It's a duplicate
                else:
                    # Different file with same name - increment and try again
                    new_name = self._increment_filename(base_new_name, counter)
                    counter += 1
                    continue
            
            # No conflicts - we can use this target path
            return new_name, target_path
    
    def _detect_content_duplicates(self, files: List[FileInfo]):
        """Detect content duplicates across all files and mark them"""
        # Group files by their target filename (without path) to find potential duplicates
        target_name_groups = defaultdict(list)
        for file_info in files:
            target_name = self.generate_new_filename(file_info)
            target_name_groups[target_name].append(file_info)
        
        # Check for duplicates within each filename group
        for target_name, file_group in target_name_groups.items():
            if len(file_group) > 1:
                # Compare each file with the others in the group
                for i, file1 in enumerate(file_group):
                    if file1.is_duplicate:  # Already marked as duplicate
                        continue
                    
                    # Check against all subsequent files in the group
                    for file2 in file_group[i+1:]:
                        if file2.is_duplicate:  # Already marked as duplicate
                            continue
                        
                        try:
                            if self.duplicate_detector.files_are_identical(file1.path, file2.path):
                                # Mark the second file as duplicate of the first
                                file2.is_duplicate = True
                                file2.duplicate_of = file1.path
                        except Exception as e:
                            file2.issues.append(f"Duplicate check failed: {e}")

    def _check_for_duplicate(self, file_info: FileInfo, target_path: pathlib.Path) -> bool:
        """Check if file is duplicate and mark it if so. Returns True if duplicate."""
        try:
            if self.duplicate_detector.files_are_identical(file_info.path, target_path):
                file_info.is_duplicate = True
                file_info.duplicate_of = target_path
                # For duplicates, we don't set a target_path since they won't be moved
                return True
        except Exception as e:
            # Log error but don't treat as duplicate
            file_info.issues.append(f"Duplicate check failed: {e}")
        return False
    
    def plan_renames(self, files: List[FileInfo]) -> Dict[str, FileInfo]:
        """Plan renames with conflict resolution"""
        if not files:
            return {}
        
        self.print_progress("Planning renames and organization...")
        
        # Sort files by date for consistent processing
        sorted_files = sorted(files, key=lambda f: f.date_created)
        
        # First pass: Detect duplicates by content across all files
        self._detect_content_duplicates(sorted_files)
        
        # Track used target paths to handle conflicts
        used_target_paths: Set[str] = set()
        planned_operations: Dict[str, FileInfo] = {}
        
        # Progress bar for planning phase
        pbar = tqdm(sorted_files, desc="Planning operations", unit="file", leave=False)
        
        for file_info in pbar:
            # Skip duplicates that were already detected in the first pass
            if file_info.is_duplicate:
                continue
                
            base_new_name = self.generate_new_filename(file_info)
            
            # Resolve naming conflicts and get final target path
            final_name, target_path = self._resolve_naming_conflicts(file_info, base_new_name, used_target_paths)
            
            # Skip processing if it's a duplicate (empty path returned)
            if str(target_path) == "." or target_path == pathlib.Path():
                continue
            
            used_target_paths.add(str(target_path))
            file_info.new_name = final_name
            file_info.target_path = target_path
            
            # Plan operation if file needs to move/rename and isn't a duplicate
            current_path_str = str(file_info.path)
            target_path_str = str(target_path)
            if current_path_str != target_path_str and not file_info.is_duplicate:
                planned_operations[current_path_str] = file_info
        
        pbar.close()
        return planned_operations
    
    def show_duplicates(self, files: List[FileInfo]):
        """Show files that are duplicates and can be safely deleted"""
        duplicates = [f for f in files if f.is_duplicate]
        
        if not duplicates:
            self.print_info("No duplicates found")
            return
        
        print()  # Empty line before duplicates
        self.print_warning(f"Found {len(duplicates)} duplicate files (can be safely deleted):")
        
        # Group by directory for cleaner display
        by_directory = defaultdict(list)
        for file_info in duplicates:
            by_directory[str(file_info.path.parent)].append(file_info)
        
        for directory, files in sorted(by_directory.items()):
            self.print_warning(f"  Directory: {directory}")
            for file_info in files:
                print(f"{Fore.YELLOW}{Style.DIM}    {file_info.original_name} (duplicate of {file_info.duplicate_of.name}){Style.RESET_ALL}")
        
        self.print_info(f"  These {len(duplicates)} files are identical to existing target files and can be safely deleted.")
        print()  # Empty line after duplicates
    
    def interactive_device_selection(self, files: List[FileInfo]):
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
        self.print_result("Camera devices found in photos:")
        self.print_info("(Photos from selected devices will be kept in regular folders)")
        print()
        
        # Display devices with numbers
        for i, (device, count) in enumerate(sorted_devices, 1):
            self.print_result(f"  [{i}] {device} ({count} photos)")
        
        print()
        self.print_config("Select family devices by entering numbers (e.g., '1 3 5') or press Enter to skip:")
        
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
                    FAMILY_DEVICES['user_selected'] = selected_devices
                    
                    # Re-run external photo detection with new devices
                    self.print_success(f"\nAdded {len(selected_devices)} device(s) as family devices")
                    self.print_tip("Tip: Use --family-devices next time with these values:")
                    for device in selected_devices:
                        self.print_tip(f'  "{device}"')
                    
                    # Re-detect external photos
                    for file_info in files:
                        file_info.is_external = False
                        file_info.external_reason = None
                        self.detect_external_photo(file_info)
        
        except KeyboardInterrupt:
            print("\nDevice selection cancelled")
    
    def show_external_photos_report(self, files: List[FileInfo]):
        """Show summary of external photos detected"""
        external_photos = [f for f in files if f.is_external]
        
        if not external_photos:
            return  # Don't show anything if no external photos
        
        print()  # Empty line before report
        self.print_result(f"External photos detected: {len(external_photos)} files")
        
        # Group by reason for cleaner display
        reason_groups = defaultdict(list)
        for file_info in external_photos:
            reason = file_info.external_reason or "Unknown reason"
            reason_groups[reason].append(file_info.original_name)
        
        for reason, filenames in sorted(reason_groups.items()):
            self.print_result(f"  {reason} ({len(filenames)} files):")
            # Show first few filenames
            show_count = min(3, len(filenames))
            for filename in filenames[:show_count]:
                print(f"{Fore.WHITE}{Style.DIM}    - {filename}{Style.RESET_ALL}")
            if len(filenames) > show_count:
                print(f"{Fore.WHITE}{Style.DIM}    - ... and {len(filenames) - show_count} more{Style.RESET_ALL}")
        
        if self.args.organize:
            self.print_info("  These files will be organized into 'extern' folders")
        print()  # Empty line after report
    
    def show_analysis_summary(self, files: List[FileInfo], duplicates_count: int):
        """Show brief summary of issues and duplicates"""
        files_with_issues = [f for f in files if f.issues]
        
        if not files_with_issues and duplicates_count == 0:
            return
        
        print()  # Empty line before summary
        
        if files_with_issues:
            self.print_warning(f"{len(files_with_issues)} files had analysis issues (see details above)")
        
        if duplicates_count > 0:
            self.print_warning(f"{duplicates_count} duplicate files found (see details above)")
    
    def prompt_operations_confirmation(self, planned_operations: Dict[str, FileInfo]) -> bool:
        """Confirm planned operations before execution"""
        if not planned_operations:
            return True
        
        print()  # Empty line before prompt
        self.print_result(f"{len(planned_operations)} files ready to process")
        
        try:
            response = input("\nProceed with these operations? [y/N]: ").strip().lower()
            return response in ['y', 'yes']
        except KeyboardInterrupt:
            print("\nOperation cancelled by user")
            return False
    
    def execute_operations(self, planned_operations: Dict[str, FileInfo]) -> bool:
        """Execute the planned file operations"""
        if not planned_operations:
            return True
        
        operation_verb = "copy" if self.args.copy else "move"
        self.print_progress(f"{operation_verb.capitalize()}ing files to their destinations...")
        
        # Create progress bar for operations
        pbar = tqdm(total=len(planned_operations), desc="Processing files", unit="files", leave=True)
        
        success_files = []
        failed_files = []
        
        for file_info in planned_operations.values():
            try:
                # Create target directory if it doesn't exist
                target_dir = file_info.target_path.parent
                target_dir.mkdir(parents=True, exist_ok=True)
                
                if self.args.copy:
                    # Copy mode - always copy, never delete original
                    shutil.copy2(file_info.path, file_info.target_path)
                else:
                    # Move mode - try rename first, fallback to copy+delete for cross-drive
                    try:
                        file_info.path.rename(file_info.target_path)
                    except OSError as rename_error:
                        if "different disk drive" in str(rename_error) or rename_error.errno == 17:
                            # Cross-drive operation - use copy + delete
                            shutil.copy2(file_info.path, file_info.target_path)
                            file_info.path.unlink()  # Delete original after successful copy
                        else:
                            raise  # Re-raise if it's a different error
                
                success_files.append(file_info.original_name)
                
            except Exception as e:
                failed_files.append((file_info.original_name, str(e)))
            
            pbar.update(1)
        
        pbar.close()
        
        # Show operation summary
        self.show_operation_summary(success_files, failed_files, operation_verb.lower())
        
        return len(failed_files) == 0
    
    def show_operation_summary(self, success_files: List[str], failed_files: List[Tuple[str, str]], operation_verb: str):
        """Show summary of file operations"""
        if success_files:
            past_tense = "copied" if self.args.copy else "moved"
            self.print_success(f"Successfully {past_tense} {len(success_files)} files")
        
        if failed_files:
            self.print_error(f"Failed to process {len(failed_files)} files:")
            # Show first few failures with details
            show_count = min(5, len(failed_files))
            for filename, error in failed_files[:show_count]:
                print(f"{Fore.RED}{Style.DIM}  - {filename}: {error}{Style.RESET_ALL}")
            if len(failed_files) > show_count:
                print(f"{Fore.RED}{Style.DIM}  - ... and {len(failed_files) - show_count} more failures{Style.RESET_ALL}")
    
    def show_rename_preview(self, planned_operations: Dict[str, FileInfo]):
        """Show preview of planned operations grouped by target directory"""
        if not planned_operations:
            self.print_result("No files need processing")
            return
        
        operation_type = "Organization" if self.args.organize else "Rename"
        self.print_result(f"\n{operation_type} preview ({len(planned_operations)} files):")
        
        # Group by target directory for cleaner display
        by_target_directory = defaultdict(list)
        for file_info in planned_operations.values():
            target_dir = str(file_info.target_path.parent)
            by_target_directory[target_dir].append(file_info)
        
        for target_dir, files in sorted(by_target_directory.items()):
            self.print_result(f"\nTarget: {target_dir}")
            for file_info in files:
                source = f"{file_info.path.parent.name}/{file_info.original_name}"
                target = file_info.target_path.name
                
                if self.args.organize:
                    # Show full path change for organization
                    print(f"{Fore.WHITE}{Style.DIM}  {source} → {target}{Style.RESET_ALL}")
                else:
                    # Show just rename
                    print(f"{Fore.WHITE}{Style.DIM}  {file_info.original_name} → {target}{Style.RESET_ALL}")
    
    def show_configuration(self):
        """Show current configuration and what will be processed"""
        self.print_config("PhotoChronos Configuration:")
        
        # Helper to print config line with dim label and colored value
        def print_config_line(label: str, value: str):
            print(f"{Fore.CYAN}{Style.DIM}  {label}: {Style.RESET_ALL}{Fore.CYAN}{value}{Style.RESET_ALL}")
        
        print_config_line("Paths", ', '.join(str(p) for p in self.args.path))
        print_config_line("Recursive", 'Yes' if self.args.recursive else 'No')
        print_config_line("Extensions", ', '.join(sorted(self.args.extension)))
        print_config_line("Dry run", 'Yes' if self.args.dry_run else 'No')
        print_config_line("Organize into folders", 'Yes' if self.args.organize else 'No')
        
        if self.args.output_dir:
            print_config_line("Output directory", str(self.args.output_dir))
        
        print_config_line("Operation mode", 'Copy' if self.args.copy else 'Move')
        
        if self.args.organize:
            print_config_line("External photo detection", "Enabled")
        
        if self.args.family_devices:
            print_config_line("Additional family devices", ', '.join(self.args.family_devices))
        
        if not WINDOWS_METADATA:
            print_config_line("Video metadata", "Limited (Windows COM not available)")
        
        # Show cache stats if there are cached hashes
        cache_stats = self.duplicate_detector.get_cache_stats()
        if cache_stats['cached_files'] > 0:
            print_config_line("Hash cache", f"{cache_stats['cached_files']} files ({cache_stats['algorithm']})")
        
        print()  # Empty line after config

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="PhotoChronos - Organize photos and videos by date",
        epilog="Renames media files to YYYYMMDD_HHMMSS format based on creation date"
    )
    
    parser.add_argument(
        'path', 
        type=pathlib.Path,
        nargs='+',
        help='Directories to process'
    )
    
    parser.add_argument(
        '-e', '--extension',
        nargs='+',
        default=list(ALL_EXTENSIONS),
        help=f'File extensions to process (default: {", ".join(sorted(ALL_EXTENSIONS))})'
    )
    
    parser.add_argument(
        '-r', '--recursive',
        action='store_true',
        help='Process subdirectories recursively'
    )
    
    parser.add_argument(
        '-d', '--dry-run',
        action='store_true',
        help='Show what would be done without making changes'
    )
    
    parser.add_argument(
        '--organize',
        action='store_true',
        help='Organize files into year/year-month folder structure'
    )
    
    parser.add_argument(
        '-o', '--output-dir',
        type=pathlib.Path,
        help='Base directory for organized output (default: same as source)'
    )
    
    parser.add_argument(
        '--copy',
        action='store_true',
        help='Copy files instead of moving them (leaves originals intact)'
    )
    
    parser.add_argument(
        '--family-devices',
        nargs='+',
        help='Additional family device patterns to recognize (e.g., "Pixel 7" "OnePlus")'
    )
    
    args = parser.parse_args()
    
    # Initialize PhotoChronos
    app = PhotoChronos(args)
    
    # Show configuration
    app.show_configuration()
    
    # Find and analyze files
    file_paths = app.find_media_files()
    
    if not file_paths:
        app.print_result("No media files found in specified directories")
        return 0
    
    app.print_success(f"Found {len(file_paths)} media files to process")
    
    # Analyze files for metadata
    files = app.analyze_files(file_paths)
    
    if not files:
        app.print_error("No files could be analyzed")
        return 1
    
    app.print_success(f"Successfully analyzed {len(files)} files")
    
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
    
    # Count duplicates for summary
    duplicates_count = sum(1 for f in files if f.is_duplicate)
    
    # Show brief summary of issues
    app.show_analysis_summary(files, duplicates_count)
    
    # Show operation preview
    app.show_rename_preview(planned_operations)
    
    if args.dry_run:
        app.print_info("\nDry run mode - no files were modified")
        return 0
    
    if not planned_operations and duplicates_count == 0:
        app.print_success("All files are already in correct locations with correct names")
        return 0
    
    # Get user confirmation before proceeding
    if not app.prompt_operations_confirmation(planned_operations):
        app.print_info("Operation cancelled by user")
        return 0
    
    # Execute the operations
    success = app.execute_operations(planned_operations)
    
    if success:
        app.print_success("File organization completed successfully!")
        if duplicates_count > 0:
            app.print_tip(f"\nRemember to manually clean up the {duplicates_count} duplicate files shown above")
        return 0
    else:
        app.print_error("Some operations failed - check error messages above")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())