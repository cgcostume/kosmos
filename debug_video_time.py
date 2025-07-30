#!/usr/bin/env python3
"""Debug video timestamp extraction"""

import pathlib
import datetime
from file_analyzer import FileAnalyzer

# Check what video files we have
test_folder = pathlib.Path("test")
video_files = list(test_folder.glob("*.mp4"))

analyzer = FileAnalyzer()

print("=== VIDEO TIMESTAMP ANALYSIS ===\n")

for video_file in video_files:
    print(f"File: {video_file.name}")
    
    # Get file system dates
    stat = video_file.stat()
    fs_created = datetime.datetime.fromtimestamp(stat.st_ctime)
    fs_modified = datetime.datetime.fromtimestamp(stat.st_mtime)
    
    print(f"File system created: {fs_created}")
    print(f"File system modified: {fs_modified}")
    
    # Analyze with our analyzer
    result = analyzer.analyze_file(video_file)
    print(f"Analyzer result: {result.date_created}")
    print(f"Timezone: {analyzer.timezone}")
    
    # Check if this is using video metadata or file system date
    if "No video metadata available" in ' '.join(result.issues):
        print("Using file system date (metadata extraction failed)")
    else:
        print("Using video metadata extraction")
    
    # Parse expected time from filename
    if len(video_file.stem) >= 15:
        try:
            expected = datetime.datetime.strptime(video_file.stem[:15], "%Y%m%d_%H%M%S")
            print(f"Expected from filename: {expected}")
            
            if result.date_created:
                # Compare times
                if hasattr(result.date_created, 'replace'):
                    naive_result = result.date_created.replace(tzinfo=None) if result.date_created.tzinfo else result.date_created
                else:
                    naive_result = result.date_created
                    
                diff = naive_result - expected
                print(f"Time difference: {diff}")
        except ValueError:
            print("Could not parse expected time from filename")
    
    if result.issues:
        print("Issues:")
        for issue in result.issues:
            print(f"  - {issue}")
    
    print("-" * 50)