#!/usr/bin/env python3
"""
Auxiliary utility functions for the Kosmos project

Provides common utility functions shared across multiple tools
like monosis, photochronos, etc.
"""

import pathlib
from typing import Optional


def format_bytes(size_bytes: int) -> str:
    """Format byte size into human-readable string

    Args:
        size_bytes: Size in bytes to format

    Returns:
        Formatted string like "1.2 GiB", "345 MiB", "12 KiB", or "789 B"
    """
    if size_bytes >= 1024**3:
        return f"{size_bytes / (1024**3):.1f} GiB"
    if size_bytes >= 1024**2:
        return f"{size_bytes / (1024**2):.1f} MiB"
    if size_bytes >= 1024:
        return f"{size_bytes / 1024:.1f} KiB"
    return f"{size_bytes} B"


def format_path_for_display(path: str, home_path: Optional[str] = None) -> str:
    """Format file path for display by replacing home directory with ~

    Args:
        path: File path to format
        home_path: Home directory path (defaults to platform home)

    Returns:
        Path with home directory replaced by ~ if applicable
    """
    if home_path is None:
        home_path = str(pathlib.Path.home())

    return path.replace(home_path, "~")


def truncate_path(path: str, max_length: int = 50) -> str:
    """Truncate long paths for display

    Args:
        path: Path to truncate
        max_length: Maximum length before truncation

    Returns:
        Truncated path with ... in the middle if too long
    """
    if len(path) <= max_length:
        return path

    # Calculate how much space we have for path parts
    available = max_length - 3  # Account for "..."

    # Split roughly in half
    start_len = available // 2
    end_len = available - start_len

    return f"{path[:start_len]}...{path[-end_len:]}"
