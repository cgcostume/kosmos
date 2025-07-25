# Kosmos

Personal file organization tools for bringing order to digital chaos.

## Tools

### PhotoChronos (`photochronos.py`)

**Status: ✅ Complete and Production-Ready**

A modern photo and video organization tool that renames files based on their creation date and optionally organizes them into year/month folder structures.

#### Features

- **Date-based renaming** - Files renamed to `YYYYMMDD_HHMMSS.ext` format
- **EXIF/metadata extraction** - Reads creation dates from image EXIF and video metadata
- **Smart folder organization** - Optional year/year-month structure (e.g., `2024/2024-12/`)
- **Intelligent conflict resolution** - Handles naming conflicts with hash-based duplicate detection
- **Cross-platform support** - Works with local drives and network shares
- **Interactive confirmation** - Preview changes before execution
- **Professional CLI** - Color-coded output, progress bars, comprehensive error handling

#### Usage

```bash
# Basic usage - rename files in place
python photochronos.py /path/to/photos

# Recursive processing with dry-run preview
python photochronos.py /path/to/photos -r -d

# Organize into year/month folders
python photochronos.py /path/to/photos --organize -d

# Organize to specific output directory
python photochronos.py /path/to/photos --organize -o /organized/photos -d

# Process specific file types only
python photochronos.py /path/to/photos -e jpg png mp4 -r -d
```

#### Options

- `-r, --recursive` - Process subdirectories recursively
- `-d, --dry-run` - Show what would be done without making changes
- `--organize` - Organize files into year/year-month folder structure
- `-o, --output-dir` - Base directory for organized output
- `-e, --extension` - File extensions to process (default: all supported formats)

#### Supported Formats

**Images:** JPG, JPEG, PNG, TIFF, BMP, GIF, WebP, SRW, RAW, CR2, NEF, ARW  
**Videos:** MP4, MOV, AVI, MKV, WMV, FLV, WebM, M4V, 3GP

### Future Tools

- **Deduplicator** - Standalone file deduplication utilities (planned)
- **Archiver** - File archival tools (planned)

## Requirements

- Python 3.12+
- Windows: Full video metadata support
- Cross-platform: Basic functionality available

## Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Test installation
python photochronos.py --help
```

## Development

PhotoChronos follows modern Python practices with comprehensive error handling, type hints, and professional CLI design. See the source code for detailed documentation and examples.