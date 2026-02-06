#!/usr/bin/env python3
"""
Generic File Analysis Module

Provides metadata extraction capabilities for various file types including
images and videos. Extracts creation dates, EXIF data, and other file properties.
"""

import datetime
import json
import pathlib
import shutil
import subprocess
import sys
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

# Third-party imports
import exifread
from tzlocal import get_localzone

# DST validation
from dst_validator import is_valid_dst_difference

# ffprobe availability (cross-platform video metadata)
FFPROBE_AVAILABLE = shutil.which("ffprobe") is not None

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
    issues: list[str] = None
    raw_metadata: dict[str, Any] = None

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
                date_modified=datetime.datetime.fromtimestamp(stat.st_mtime),
            )

            # Try to extract better creation date from metadata
            creation_date = self._extract_creation_date(file_path, result)
            if creation_date:
                result.date_created = creation_date
            # If metadata extraction failed, prefer modification date for both images and videos
            # (creation date gets updated when files are copied/moved)
            elif self._is_image_file(file_path):
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
                issues=[f"Analysis failed: {e}"],
            )

    def analyze_files(self, file_paths: list[pathlib.Path]) -> list[FileAnalysisResult]:
        """Analyze multiple files and return results"""
        return [self.analyze_file(path) for path in file_paths]

    def _extract_creation_date(
        self, file_path: pathlib.Path, result: FileAnalysisResult
    ) -> Optional[datetime.datetime]:
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

    def _extract_date_from_image(
        self, file_path: pathlib.Path, result: FileAnalysisResult
    ) -> Optional[datetime.datetime]:
        """Extract creation date from image EXIF data"""
        try:
            with open(file_path, "rb") as f:
                tags = exifread.process_file(f, stop_tag="DateTime")

                if tags:
                    result.has_exif = True
                    result.raw_metadata.update({str(k): str(v) for k, v in tags.items()})

                    # Extract camera info
                    if "Image Make" in tags:
                        result.camera_make = str(tags["Image Make"]).strip()
                    if "Image Model" in tags:
                        result.camera_model = str(tags["Image Model"]).strip()

                # Try multiple date fields in order of preference
                date_tags = ["EXIF DateTimeOriginal", "EXIF DateTime", "Image DateTime"]

                for tag_name in date_tags:
                    if tag_name in tags:
                        try:
                            date_str = str(tags[tag_name]).strip()[:19]
                            try:
                                return datetime.datetime.strptime(date_str, "%Y:%m:%d %H:%M:%S")
                            except ValueError:
                                # Try clamping out-of-range seconds (some cameras write invalid values)
                                parts = date_str.split(":")
                                if len(parts) == 5:
                                    seconds = min(int(parts[4]), 59)
                                    parts[4] = f"{seconds:02d}"
                                    return datetime.datetime.strptime(":".join(parts), "%Y:%m:%d %H:%M:%S")
                                raise
                        except ValueError as e:
                            result.issues.append(f"Invalid date format in {tag_name}: {e}")
                            continue

        except Exception as e:
            result.issues.append(f"EXIF extraction failed: {e}")

        return None

    def _extract_date_from_video(
        self, file_path: pathlib.Path, result: FileAnalysisResult
    ) -> Optional[datetime.datetime]:
        """Extract creation date from video metadata with validation against file system dates.

        Tries ffprobe first (cross-platform), then Windows COM as fallback.
        """
        # Try ffprobe first (works on any platform with ffmpeg installed)
        if FFPROBE_AVAILABLE:
            date = self._extract_date_from_video_ffprobe(file_path, result)
            if date:
                return date

        # Fall back to Windows COM
        if WINDOWS_METADATA:
            date = self._extract_date_from_video_windows(file_path, result)
            if date:
                return date

        if not FFPROBE_AVAILABLE and not WINDOWS_METADATA:
            result.issues.append("Video metadata not available (no ffprobe or Windows COM)")

        return None

    def _extract_date_from_video_ffprobe(
        self, file_path: pathlib.Path, result: FileAnalysisResult
    ) -> Optional[datetime.datetime]:
        """Extract creation date from video metadata using ffprobe"""
        try:
            proc = subprocess.run(
                ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_format", "-show_streams", str(file_path)],
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )

            if proc.returncode != 0:
                return None

            probe_data = json.loads(proc.stdout)

            # Look for creation_time in format tags first, then stream tags
            creation_time_str = None

            format_tags = probe_data.get("format", {}).get("tags", {})
            # Tag keys can vary in case
            for key, value in format_tags.items():
                if key.lower() == "creation_time":
                    creation_time_str = value
                    break

            if not creation_time_str:
                for stream in probe_data.get("streams", []):
                    stream_tags = stream.get("tags", {})
                    for key, value in stream_tags.items():
                        if key.lower() == "creation_time":
                            creation_time_str = value
                            break
                    if creation_time_str:
                        break

            if not creation_time_str:
                return None

            # Parse the UTC timestamp (typically ISO 8601: 2025-07-05T18:36:10.000000Z)
            # Strip trailing Z and fractional seconds for consistent parsing
            clean_str = creation_time_str.replace("Z", "+00:00")
            utc_dt = datetime.datetime.fromisoformat(clean_str)

            # Convert UTC to local naive datetime
            if utc_dt.tzinfo is not None:
                local_dt = utc_dt.astimezone(self.timezone)
                naive_date = local_dt.replace(tzinfo=None)
            else:
                naive_date = utc_dt

            # Validate against file system dates (same logic as Windows path)
            return self._validate_video_date(naive_date, file_path, result)

        except subprocess.TimeoutExpired:
            result.issues.append("ffprobe timed out reading video metadata")
        except (json.JSONDecodeError, ValueError, KeyError) as e:
            result.issues.append(f"Could not parse ffprobe output: {e}")
        except Exception as e:
            result.issues.append(f"ffprobe extraction failed: {e}")

        return None

    def _extract_date_from_video_windows(
        self, file_path: pathlib.Path, result: FileAnalysisResult
    ) -> Optional[datetime.datetime]:
        """Extract creation date from video metadata using Windows COM"""
        try:
            from win32com.propsys import propsys, pscon

            properties = propsys.SHGetPropertyStoreFromParsingName(str(file_path.absolute()))
            date_created = properties.GetValue(pscon.PKEY_Media_DateEncoded).GetValue()

            if isinstance(date_created, datetime.datetime):
                # Convert to naive datetime (remove timezone info to avoid conversion issues)
                if date_created.tzinfo is not None:
                    naive_date = date_created.replace(tzinfo=None)
                else:
                    naive_date = date_created

                return self._validate_video_date(naive_date, file_path, result)

        except Exception as e:
            result.issues.append(f"Could not read video metadata: {e}")

        return None

    def _validate_video_date(
        self,
        naive_date: datetime.datetime,
        file_path: pathlib.Path,
        result: FileAnalysisResult,
    ) -> datetime.datetime:
        """Validate extracted video date against file system dates and filename"""
        file_modified = result.date_modified
        file_created = result.date_created

        # Check against file system date for timezone issues
        is_valid_fs, explanation_fs = is_valid_dst_difference(naive_date, file_modified)
        if is_valid_fs:
            result.issues.append(f"Video metadata timezone issue: {explanation_fs}, using file system date")
            return file_modified

        # Check against filename time if parseable
        try:
            filename_stem = file_path.stem
            if len(filename_stem) >= 15 and filename_stem[:8].isdigit() and filename_stem[9:15].isdigit():
                expected_time = datetime.datetime.strptime(filename_stem[:15], "%Y%m%d_%H%M%S")
                is_valid_fn, explanation_fn = is_valid_dst_difference(naive_date, expected_time)
                if is_valid_fn:
                    result.issues.append(f"Video metadata timezone issue: {explanation_fn}, using filename time")
                    return expected_time
        except (ValueError, IndexError):
            pass  # Filename not in expected format

        # Check if metadata date is significantly newer (file was copied after creation)
        if naive_date > file_modified + datetime.timedelta(days=1):
            result.issues.append(
                f"Video metadata date ({naive_date.strftime('%Y-%m-%d')}) newer than file modified date ({file_modified.strftime('%Y-%m-%d')}), using file system date"
            )
            return min(file_created, file_modified)

        return naive_date

    def _is_image_file(self, file_path: pathlib.Path) -> bool:
        """Check if file is an image based on extension"""
        image_extensions = {
            "jpg",
            "jpeg",
            "png",
            "gif",
            "bmp",
            "tiff",
            "tif",
            "raw",
            "cr2",
            "nef",
            "arw",
            "srw",
            "webp",
        }
        return file_path.suffix.lower().lstrip(".") in image_extensions

    def _is_video_file(self, file_path: pathlib.Path) -> bool:
        """Check if file is a video based on extension"""
        video_extensions = {"mp4", "avi", "mov", "mkv", "wmv", "flv", "webm", "m4v", "3gp"}
        return file_path.suffix.lower().lstrip(".") in video_extensions
