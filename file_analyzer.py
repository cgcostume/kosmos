#!/usr/bin/env python3
"""
Generic File Analysis Module

Provides metadata extraction capabilities for various file types including
images and videos. Extracts creation dates, EXIF data, and other file properties.
"""

import pathlib
import datetime
import sys
from typing import Optional, List, Dict, Any
from dataclasses import dataclass

# Third-party imports
import exifread
from tzlocal import get_localzone

# Windows-specific imports for video metadata
try:
    from win32com.propsys import propsys, pscon
    WINDOWS_METADATA = True
except ImportError:
    WINDOWS_METADATA = False


@dataclass
class FileAnalysisResult:
    """Result of file analysis containing metadata and extracted information"""
    path: pathlib.Path
    file_size: int
    date_created: datetime.datetime
    date_modified: datetime.datetime
    has_exif: bool = False
    camera_make: Optional[str] = None
    camera_model: Optional[str] = None
    issues: List[str] = None
    raw_metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.issues is None:
            self.issues = []
        if self.raw_metadata is None:
            self.raw_metadata = {}


class FileAnalyzer:
    """Generic file analyzer for extracting metadata from various file types"""
    
    def __init__(self, timezone=None):
        """Initialize analyzer with optional timezone"""
        self.timezone = timezone or get_localzone()
    
    def analyze_file(self, file_path: pathlib.Path) -> FileAnalysisResult:
        """Analyze a single file and extract all available metadata"""
        try:
            stat = file_path.stat()
            
            # Create base result with file system info (naive datetime objects)
            result = FileAnalysisResult(
                path=file_path,
                file_size=stat.st_size,
                date_created=datetime.datetime.fromtimestamp(stat.st_ctime),
                date_modified=datetime.datetime.fromtimestamp(stat.st_mtime)
            )
            
            # Try to extract better creation date from metadata
            creation_date = self._extract_creation_date(file_path, result)
            if creation_date:
                result.date_created = creation_date
            else:
                # If metadata extraction failed, prefer modification date for both images and videos
                # (creation date gets updated when files are copied/moved)
                if self._is_image_file(file_path):
                    result.date_created = result.date_modified
                    result.issues.append("No image EXIF data available, using file modification date")
                elif self._is_video_file(file_path):
                    result.date_created = result.date_modified
                    result.issues.append("No video metadata available, using file modification date")
            
            return result
            
        except Exception as e:
            # Return minimal result with error
            return FileAnalysisResult(
                path=file_path,
                file_size=0,
                date_created=datetime.datetime.now(),
                date_modified=datetime.datetime.now(),
                issues=[f"Analysis failed: {e}"]
            )
    
    def analyze_files(self, file_paths: List[pathlib.Path]) -> List[FileAnalysisResult]:
        """Analyze multiple files and return results"""
        return [self.analyze_file(path) for path in file_paths]
    
    def _extract_creation_date(self, file_path: pathlib.Path, result: FileAnalysisResult) -> Optional[datetime.datetime]:
        """Extract creation date from file metadata"""
        # Try image metadata first
        if self._is_image_file(file_path):
            date = self._extract_date_from_image(file_path, result)
            if date:
                return date
        
        # Try video metadata
        if self._is_video_file(file_path):
            date = self._extract_date_from_video(file_path, result)
            if date:
                return date
        
        return None
    
    def _extract_date_from_image(self, file_path: pathlib.Path, result: FileAnalysisResult) -> Optional[datetime.datetime]:
        """Extract creation date from image EXIF data"""
        try:
            with open(file_path, 'rb') as f:
                tags = exifread.process_file(f, stop_tag='DateTime')
                
                if tags:
                    result.has_exif = True
                    result.raw_metadata.update({str(k): str(v) for k, v in tags.items()})
                    
                    # Extract camera info
                    if 'Image Make' in tags:
                        result.camera_make = str(tags['Image Make']).strip()
                    if 'Image Model' in tags:
                        result.camera_model = str(tags['Image Model']).strip()
                
                # Try multiple date fields in order of preference
                date_tags = [
                    'EXIF DateTimeOriginal',
                    'EXIF DateTime', 
                    'Image DateTime'
                ]
                
                for tag_name in date_tags:
                    if tag_name in tags:
                        try:
                            date_str = str(tags[tag_name])
                            return datetime.datetime.strptime(date_str, "%Y:%m:%d %H:%M:%S")
                        except ValueError as e:
                            result.issues.append(f"Invalid date format in {tag_name}: {e}")
                            continue
                
        except Exception as e:
            result.issues.append(f"EXIF extraction failed: {e}")
        
        return None
    
    def _extract_date_from_video(self, file_path: pathlib.Path, result: FileAnalysisResult) -> Optional[datetime.datetime]:
        """Extract creation date from video metadata with validation against file system dates"""
        if not WINDOWS_METADATA:
            result.issues.append("Video metadata not available (Windows COM required)")
            return None
        
        try:
            # Windows-specific video metadata extraction
            from win32com.propsys import propsys, pscon
            
            properties = propsys.SHGetPropertyStoreFromParsingName(str(file_path.absolute()))
            date_created = properties.GetValue(pscon.PKEY_Media_DateEncoded).GetValue()
            
            if isinstance(date_created, datetime.datetime):
                # Convert to naive datetime (remove timezone info to avoid conversion issues)
                if date_created.tzinfo is not None:
                    naive_date = date_created.replace(tzinfo=None)
                else:
                    naive_date = date_created
                
                # Validate against file system dates
                file_modified = result.date_modified
                file_created = result.date_created
                
                # Check for DST/timezone issues (exactly 1 hour difference)
                time_diff_seconds = abs((naive_date - file_modified).total_seconds())
                if time_diff_seconds == 3600:  # Exactly 1 hour difference
                    result.issues.append(f"Video metadata has DST/timezone issue (1h diff), using file system date")
                    return file_modified
                
                # Check if metadata date is significantly newer (file was copied after creation)
                if naive_date > file_modified + datetime.timedelta(days=1):
                    result.issues.append(f"Video metadata date ({naive_date.strftime('%Y-%m-%d')}) newer than file modified date ({file_modified.strftime('%Y-%m-%d')}), using file system date")
                    return min(file_created, file_modified)
                
                return naive_date
                
        except Exception as e:
            result.issues.append(f"Could not read video metadata: {e}")
        
        return None
    
    def _is_image_file(self, file_path: pathlib.Path) -> bool:
        """Check if file is an image based on extension"""
        image_extensions = {
            'jpg', 'jpeg', 'png', 'gif', 'bmp', 'tiff', 'tif', 
            'raw', 'cr2', 'nef', 'arw', 'srw', 'webp'
        }
        return file_path.suffix.lower().lstrip('.') in image_extensions
    
    def _is_video_file(self, file_path: pathlib.Path) -> bool:
        """Check if file is a video based on extension"""
        video_extensions = {
            'mp4', 'avi', 'mov', 'mkv', 'wmv', 'flv', 
            'webm', 'm4v', '3gp'
        }
        return file_path.suffix.lower().lstrip('.') in video_extensions