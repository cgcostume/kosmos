#!/usr/bin/env python3
"""
Configuration management for Monosis

Wrapper around shared kosmos configuration for monosis-specific settings.
Maintains backward compatibility while using shared infrastructure.
"""

import json
import os
import pathlib
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Optional

# Use shared kosmos configuration
from kosmos_config import SharedConfigManager, init_shared_cache_db


@dataclass
class MonosisConfig:
    """Configuration for Monosis"""

    source_locations: list[str]
    target_location: Optional[str]
    reference_location: Optional[str]
    last_scan: Optional[str]
    last_consolidation: Optional[str]
    min_file_size: int  # Minimum file size in bytes
    ignore_patterns: list[str]  # Glob patterns to ignore
    max_workers: int  # Maximum number of parallel hashing threads
    cache_batch_size: int  # Number of hashes to compute before saving to database

    def add_source(self, path: pathlib.Path) -> bool:
        """Add a source location"""
        path_str = str(path.resolve())
        if path_str not in self.source_locations:
            self.source_locations.append(path_str)
            return True
        return False

    def remove_source(self, path: pathlib.Path) -> bool:
        """Remove a source location"""
        path_str = str(path.resolve())
        if path_str in self.source_locations:
            self.source_locations.remove(path_str)
            return True
        return False

    def set_target(self, path: pathlib.Path):
        """Set target location"""
        self.target_location = str(path.resolve())

    def set_reference(self, path: pathlib.Path):
        """Set reference location"""
        self.reference_location = str(path.resolve())

    def get_source_paths(self) -> list[pathlib.Path]:
        """Get source locations as Path objects"""
        return [pathlib.Path(p) for p in self.source_locations]

    def get_target_path(self) -> Optional[pathlib.Path]:
        """Get target location as Path object"""
        return pathlib.Path(self.target_location) if self.target_location else None

    def get_reference_path(self) -> Optional[pathlib.Path]:
        """Get reference location as Path object"""
        return pathlib.Path(self.reference_location) if self.reference_location else None

    def update_scan_time(self):
        """Update last scan timestamp"""
        self.last_scan = datetime.now(timezone.utc).isoformat()

    def update_consolidation_time(self):
        """Update last consolidation timestamp"""
        self.last_consolidation = datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> dict:
        """Convert to dictionary"""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "MonosisConfig":
        """Create from dictionary"""
        return cls(
            source_locations=data.get("source_locations", []),
            target_location=data.get("target_location"),
            reference_location=data.get("reference_location"),
            last_scan=data.get("last_scan"),
            last_consolidation=data.get("last_consolidation"),
            min_file_size=data.get("min_file_size", 1024),  # Default 1KB
            ignore_patterns=data.get("ignore_patterns", cls._default_ignore_patterns()),
            max_workers=data.get("max_workers", min(32, (os.cpu_count() or 1) + 4)),  # Default based on CPU cores
            cache_batch_size=data.get("cache_batch_size", 10000),  # Default 10,000 hashes per batch
        )

    @classmethod
    def _default_ignore_patterns(cls) -> list[str]:
        """Get default ignore patterns"""
        return [
            # System files
            "Thumbs.db",
            ".DS_Store",
            "desktop.ini",
            "Icon\r",  # macOS custom icons
            # Development
            "node_modules/**",
            "__pycache__/**",
            "*.pyc",
            ".pytest_cache/**",
            # Temporary files
            "*.tmp",
            "*.temp",
            "~*",  # Office temp files
            ".~*",  # LibreOffice temp files
            # Cache files
            ".cache/**",
            "*.cache",
            # Browser cache/temp
            "*/Cache/**",
            "*/cache/**",
        ]

    @classmethod
    def default(cls) -> "MonosisConfig":
        """Create default configuration"""
        return cls(
            source_locations=[],
            target_location=None,
            reference_location=None,
            last_scan=None,
            last_consolidation=None,
            min_file_size=1024,  # 1KB minimum
            ignore_patterns=cls._default_ignore_patterns(),
            max_workers=min(32, (os.cpu_count() or 1) + 4),  # Default based on CPU cores
            cache_batch_size=10000,  # Default 10,000 hashes per batch
        )


class ConfigManager:
    """Manages loading and saving configuration using shared kosmos infrastructure"""

    def __init__(self, config_dir: Optional[pathlib.Path] = None):
        """Initialize configuration manager

        Args:
            config_dir: Ignored - kept for backward compatibility. Uses .kosmos now.
        """
        # Use shared config manager
        self.shared_manager = SharedConfigManager()
        self.config_dir = self.shared_manager.kosmos_dir  # For compatibility
        self.config_file = self.shared_manager.config_file  # For compatibility

        # Initialize shared cache database
        init_shared_cache_db(self.shared_manager.get_cache_db_path())

        # Migrate old monosis config if exists
        self.shared_manager.migrate_from_monosis()

    def load(self) -> MonosisConfig:
        """Load configuration from shared kosmos config"""
        kosmos_config = self.shared_manager.load()

        # Get monosis-specific config
        monosis_data = kosmos_config.monosis

        if monosis_data:
            return MonosisConfig.from_dict(monosis_data)
        return MonosisConfig.default()

    def save(self, config: MonosisConfig):
        """Save configuration to shared kosmos config"""
        kosmos_config = self.shared_manager.load()

        # Update monosis section
        kosmos_config.monosis = config.to_dict()

        # Save back to shared config
        self.shared_manager.save(kosmos_config)

    def get_cache_db_path(self) -> pathlib.Path:
        """Get path to shared hash cache database"""
        return self.shared_manager.get_cache_db_path()

    def reset(self):
        """Reset configuration to default"""
        if self.config_file.exists():
            self.config_file.unlink()
