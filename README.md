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
- **External photo detection** - Automatically identifies photos from non-family sources (WhatsApp, downloads, etc.) and organizes them into separate "extern" folders
- **Intelligent conflict resolution** - Handles naming conflicts with hash-based duplicate detection
- **Cross-platform support** - Works with local drives and network shares
- **Interactive confirmation** - Preview changes before execution
- **Modern CLI** - Rich-based interface with color-coded output, tables, and comprehensive error handling
- **Copy mode** - Option to copy files instead of moving them

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

# Copy files instead of moving them
python photochronos.py /path/to/photos --organize --copy -o /backup/photos

# External photo detection with custom family devices
python photochronos.py /path/to/photos --organize --family-devices "Pixel 7" "OnePlus 9"
```

#### Options

- `-r, --recursive` - Process subdirectories recursively
- `-d, --dry-run` - Show what would be done without making changes
- `--organize` - Organize files into year/year-month folder structure
- `-o, --output-dir` - Base directory for organized output
- `-e, --extension` - File extensions to process (default: all supported formats)
- `--copy` - Copy files instead of moving them (leaves originals intact)
- `--family-devices` - Additional family device patterns to recognize (e.g., "Pixel 7")

#### External Photo Detection

When using the `--organize` flag, PhotoChronos automatically detects photos from external sources (not taken by family members) and organizes them into separate folders with an "extern" suffix (e.g., `2024/2024-12 extern/`).

**Detection Methods:**
1. **Camera Model Check** - Identifies known family devices (iPhones, Samsung Galaxy, common cameras)
2. **Messaging App Detection** - Recognizes photos from WhatsApp, Signal, Telegram, etc.
3. **EXIF Completeness** - Photos with minimal metadata are flagged as likely downloaded/shared

**Customization:**
- **Interactive Mode**: When using `--organize` without `--family-devices`, PhotoChronos will show all detected camera models and let you interactively select which belong to family members
- Use `--family-devices` to add your specific device models and skip interactive selection
- Pre-configured for common Apple, Samsung, and camera brands
- Edit `FAMILY_DEVICES` in the source code for permanent customization

**Interactive Device Selection Example:**
```
Camera devices found in photos:
Photos from family devices go to regular folders, others go to 'extern' folders

  [1] Apple iPhone 14 Pro (523 photos)
  [2] Samsung Galaxy S24 (312 photos)
  [3] Canon EOS R5 (89 photos)
  [4] Unknown Device (45 photos)

Select family devices by entering numbers (e.g., '1 3 5') or press Enter to skip:
> 1 2 3

Added 3 device(s) as family devices
```

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