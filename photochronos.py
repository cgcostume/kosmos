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
import hashlib
import shutil

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
HASH_CHUNK_SIZE = 4096
COUNTER_FORMAT = "02d"

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

class PhotoChronos:
    """Main application class for photo/video organization"""
    
    def __init__(self, args):
        self.args = args
        self.files: List[FileInfo] = []
        self.duplicates: Dict[str, List[FileInfo]] = defaultdict(list)
        
        # Validate inputs during initialization
        self._validate_inputs()
        
    def print_result(self, message: str):
        """Print primary result/output in white"""
        print(f"{Fore.WHITE}{message}{Style.RESET_ALL}")
    
    def print_config(self, message: str):
        """Print configuration info in dim blue"""
        print(f"{Fore.LIGHTBLUE_EX}{Style.DIM}{message}{Style.RESET_ALL}")
    
    def print_progress(self, message: str):
        """Print intermediate/progress messages in dim white"""
        print(f"{Fore.WHITE}{Style.DIM}{message}{Style.RESET_ALL}")
    
    def print_success(self, message: str):
        """Print success messages in green"""
        print(f"{Fore.GREEN}{message}{Style.RESET_ALL}")
    
    def print_warning(self, message: str):
        """Print warning messages in yellow"""
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
        
        for path in self.args.path:
            path_obj = pathlib.Path(path)
            if not path_obj.exists():
                self.print_warning(f"Path does not exist: {path}")
                continue
                
            # Use pathlib for better cross-platform support
            pattern = "**/*" if self.args.recursive else "*"
            
            for file_path in path_obj.glob(pattern):
                if file_path.is_file() and file_path.suffix.lower().lstrip('.') in extensions:
                    files.append(file_path)
        
        return files

    def extract_date_from_image(self, file_path: pathlib.Path) -> Optional[datetime.datetime]:
        """Extract creation date from image EXIF data"""
        date_tags = [
            'EXIF DateTimeOriginal', 'DateTimeOriginal',
            'EXIF DateTimeDigitized', 'DateTimeDigitized', 
            'EXIF DateTime', 'DateTime'
        ]
        
        try:
            with open(file_path, 'rb') as file:
                tags = exifread.process_file(file, details=False)
                
                for tag in date_tags:
                    if tag in tags:
                        date_str = str(tags[tag])
                        try:
                            return datetime.datetime.strptime(date_str, '%Y:%m:%d %H:%M:%S')
                        except ValueError:
                            continue
                            
        except Exception as e:
            self.print_warning(f"Could not read EXIF from {file_path.name}: {e}")
        
        return None

    def extract_date_from_video(self, file_path: pathlib.Path) -> Optional[datetime.datetime]:
        """Extract creation date from video metadata (Windows only)"""
        if not WINDOWS_METADATA:
            return None
            
        try:
            properties = propsys.SHGetPropertyStoreFromParsingName(str(file_path.absolute()))
            date_created = properties.GetValue(pscon.PKEY_Media_DateEncoded).GetValue()
            
            if isinstance(date_created, datetime.datetime):
                # Convert to local timezone and make naive for consistency
                local_date = date_created.astimezone(get_localzone())
                return local_date.replace(tzinfo=None)
                
        except Exception as e:
            self.print_warning(f"Could not read video metadata from {file_path.name}: {e}")
        
        return None

    def get_file_date(self, file_path: pathlib.Path) -> datetime.datetime:
        """Get the creation date of a file, trying multiple methods"""
        file_ext = file_path.suffix.lower().lstrip('.')
        
        # Try EXIF for images
        if file_ext in IMAGE_EXTENSIONS:
            exif_date = self.extract_date_from_image(file_path)
            if exif_date:
                return exif_date
        
        # Try video metadata for videos
        elif file_ext in VIDEO_EXTENSIONS:
            video_date = self.extract_date_from_video(file_path)
            if video_date:
                return video_date
        
        # Fallback to file modification time
        return datetime.datetime.fromtimestamp(file_path.stat().st_mtime)

    def analyze_files(self, file_paths: List[pathlib.Path]) -> List[FileInfo]:
        """Analyze files and extract metadata"""
        if not file_paths:
            self.print_error("No files found to process")
            return []
        
        files = []
        pbar = tqdm(total=len(file_paths), desc="Analyzing dates", unit="files", leave=True)
        
        for file_path in file_paths:
            try:
                file_ext = file_path.suffix.lower().lstrip('.')
                file_type = 'image' if file_ext in IMAGE_EXTENSIONS else 'video'
                
                file_info = FileInfo(
                    path=file_path,
                    original_name=file_path.name,
                    file_size=file_path.stat().st_size,
                    date_created=self.get_file_date(file_path),
                    file_type=file_type
                )
                
                files.append(file_info)
                
            except Exception as e:
                self.print_warning(f"Error analyzing {file_path.name}: {e}")
            
            pbar.update(1)
        
        pbar.close()
        return files
    
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
            # Keep in same directory
            return file_info.path.parent / new_filename
        
        # Organize into year/year-month structure (e.g., 2024/2024-12)
        date = file_info.date_created
        year = date.strftime("%Y")
        year_month = date.strftime("%Y-%m")
        
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
    
    def calculate_file_hash(self, file_path: pathlib.Path) -> str:
        """Calculate MD5 hash of file for duplicate detection"""
        hash_md5 = hashlib.md5()
        try:
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(HASH_CHUNK_SIZE), b''):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception as e:
            self.print_warning(f"Could not calculate hash for {file_path.name}: {e}")
            return ""
    
    def files_are_identical(self, file1: pathlib.Path, file2: pathlib.Path) -> bool:
        """Check if two files are identical by comparing size and hash"""
        try:
            # Quick size check first
            if file1.stat().st_size != file2.stat().st_size:
                return False
            
            # Then hash check
            hash1 = self.calculate_file_hash(file1)
            hash2 = self.calculate_file_hash(file2)
            return hash1 == hash2 and hash1 != ""
        except Exception:
            return False
    
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
                new_name = self._increment_filename(base_new_name, counter)
                counter += 1
                continue
            
            # Check if target file already exists on disk
            if target_path.exists():
                # Check if it's the same file by hash
                duplicate_result = self._check_for_duplicate(file_info, target_path)
                if duplicate_result:
                    return new_name, target_path  # It's a duplicate
                else:
                    # Different file with same name - increment and try again
                    new_name = self._increment_filename(base_new_name, counter)
                    counter += 1
                    continue
            
            # No conflicts - we can use this target path
            return new_name, target_path
    
    def _check_for_duplicate(self, file_info: FileInfo, target_path: pathlib.Path) -> bool:
        """Check if file is duplicate and mark it if so. Returns True if duplicate."""
        if self.files_are_identical(file_info.path, target_path):
            file_info.is_duplicate = True
            file_info.duplicate_of = target_path
            file_info.target_path = target_path
            return True
        return False
    
    def plan_renames(self, files: List[FileInfo]) -> Dict[str, FileInfo]:
        """Plan renames with conflict resolution"""
        if not files:
            return {}
        
        self.print_progress("Planning renames and organization...")
        
        # Sort files by date for consistent processing
        sorted_files = sorted(files, key=lambda f: f.date_created)
        
        # Track used target paths to handle conflicts
        used_target_paths: Set[str] = set()
        planned_operations: Dict[str, FileInfo] = {}
        
        for file_info in sorted_files:
            base_new_name = self.generate_new_filename(file_info)
            
            # Resolve naming conflicts and get final target path
            final_name, target_path = self._resolve_naming_conflicts(file_info, base_new_name, used_target_paths)
            
            used_target_paths.add(str(target_path))
            file_info.new_name = final_name
            file_info.target_path = target_path
            
            # Plan operation if file needs to move/rename and isn't a duplicate
            current_path_str = str(file_info.path)
            target_path_str = str(target_path)
            if current_path_str != target_path_str and not file_info.is_duplicate:
                planned_operations[current_path_str] = file_info
        
        return planned_operations
    
    def show_duplicates(self, files: List[FileInfo]):
        """Show files that are duplicates and can be safely deleted"""
        duplicates = [f for f in files if f.is_duplicate]
        
        if not duplicates:
            return
        
        self.print_warning(f"\nFound {len(duplicates)} duplicate files (can be safely deleted):")
        
        # Group by directory for cleaner display
        by_directory = defaultdict(list)
        for file_info in duplicates:
            by_directory[str(file_info.path.parent)].append(file_info)
        
        for directory, files in sorted(by_directory.items()):
            self.print_config(f"\nDirectory: {directory}")
            for file_info in files:
                self.print_warning(f"  {file_info.original_name} (duplicate of {file_info.duplicate_of.name})")
        
        self.print_config(f"\nThese {len(duplicates)} files are identical to files that already exist in the target locations.")
        self.print_config("They can be safely deleted after the organization is complete.")
    
    def prompt_user_confirmation(self, planned_operations: Dict[str, FileInfo], duplicates_count: int) -> bool:
        """Prompt user for confirmation before proceeding"""
        if not planned_operations and duplicates_count == 0:
            return True
        
        print()  # Empty line before prompt
        
        if planned_operations:
            self.print_result(f"Ready to process {len(planned_operations)} files")
        
        if duplicates_count > 0:
            self.print_config(f"Found {duplicates_count} duplicates that will be left for manual cleanup")
        
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
        
        self.print_progress("Executing file operations...")
        
        # Create progress bar for operations
        pbar = tqdm(total=len(planned_operations), desc="Processing files", unit="files", leave=True)
        
        success_count = 0
        error_count = 0
        
        for file_info in planned_operations.values():
            try:
                # Create target directory if it doesn't exist
                target_dir = file_info.target_path.parent
                target_dir.mkdir(parents=True, exist_ok=True)
                
                # Try rename first (faster for same drive), fallback to copy+delete for cross-drive
                try:
                    file_info.path.rename(file_info.target_path)
                except OSError as rename_error:
                    if "different disk drive" in str(rename_error) or rename_error.errno == 17:
                        # Cross-drive operation - use copy + delete
                        shutil.copy2(file_info.path, file_info.target_path)
                        file_info.path.unlink()  # Delete original after successful copy
                    else:
                        raise  # Re-raise if it's a different error
                
                success_count += 1
                
            except Exception as e:
                self.print_error(f"Failed to process {file_info.original_name}: {e}")
                error_count += 1
            
            pbar.update(1)
        
        pbar.close()
        
        # Report results
        if success_count > 0:
            self.print_success(f"Successfully processed {success_count} files")
        
        if error_count > 0:
            self.print_error(f"Failed to process {error_count} files")
            return False
        
        return True
    
    def show_rename_preview(self, planned_operations: Dict[str, FileInfo]):
        """Show preview of planned operations grouped by target directory"""
        if not planned_operations:
            self.print_result("No files need processing")
            return
        
        operation_type = "organization" if self.args.organize else "renames"
        self.print_result(f"\nPlanned {operation_type} ({len(planned_operations)} files):")
        
        # Group by target directory for cleaner display
        by_target_directory = defaultdict(list)
        for file_info in planned_operations.values():
            target_dir = str(file_info.target_path.parent)
            by_target_directory[target_dir].append(file_info)
        
        for target_dir, files in sorted(by_target_directory.items()):
            self.print_config(f"\nTarget: {target_dir}")
            for file_info in files:
                source = f"{file_info.path.parent.name}/{file_info.original_name}"
                target = file_info.target_path.name
                
                if self.args.organize:
                    # Show full path change for organization
                    self.print_result(f"  {source} → {target}")
                else:
                    # Show just rename
                    self.print_result(f"  {file_info.original_name} → {target}")
    
    def show_configuration(self):
        """Show current configuration and what will be processed"""
        self.print_config("PhotoChronos Configuration:")
        self.print_config(f"  Paths: {', '.join(str(p) for p in self.args.path)}")
        self.print_config(f"  Recursive: {'Yes' if self.args.recursive else 'No'}")
        self.print_config(f"  Extensions: {', '.join(sorted(self.args.extension))}")
        self.print_config(f"  Dry run: {'Yes' if self.args.dry_run else 'No'}")
        self.print_config(f"  Organize into folders: {'Yes' if self.args.organize else 'No'}")
        if self.args.output_dir:
            self.print_config(f"  Output directory: {self.args.output_dir}")
        if not WINDOWS_METADATA:
            self.print_config("  Video metadata: Limited (Windows COM not available)")
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
    
    app.print_success(f"Found {len(file_paths)} media files")
    
    # Analyze files for metadata
    files = app.analyze_files(file_paths)
    
    if not files:
        app.print_error("No files could be analyzed")
        return 1
    
    app.print_success(f"Successfully analyzed {len(files)} files")
    
    # Plan operations (renames and/or organization)
    planned_operations = app.plan_renames(files)
    
    # Show duplicates that can be deleted
    app.show_duplicates(files)
    
    # Show preview of planned changes
    app.show_rename_preview(planned_operations)
    
    if args.dry_run:
        app.print_config("\nDry run mode - no files were modified")
        return 0
    
    # Count duplicates for confirmation prompt
    duplicates_count = sum(1 for f in files if f.is_duplicate)
    
    if not planned_operations and duplicates_count == 0:
        app.print_success("All files are already in correct locations with correct names")
        return 0
    
    # Get user confirmation
    if not app.prompt_user_confirmation(planned_operations, duplicates_count):
        app.print_config("Operation cancelled by user")
        return 0
    
    # Execute the operations
    success = app.execute_operations(planned_operations)
    
    if success:
        app.print_success("File organization completed successfully!")
        if duplicates_count > 0:
            app.print_config(f"\nRemember to manually clean up the {duplicates_count} duplicate files shown above")
        return 0
    else:
        app.print_error("Some operations failed - check error messages above")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())