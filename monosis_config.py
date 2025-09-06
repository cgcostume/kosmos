#!/usr/bin/env python3
"""
Configuration management for Monosis

Handles persistent storage of source locations, target location,
and other configuration settings.
"""

import json
import pathlib
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Optional


@dataclass
class MonosisConfig:
    """Configuration for Monosis"""

    source_locations: list[str]
    target_location: Optional[str]
    last_scan: Optional[str]
    last_consolidation: Optional[str]

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

    def get_source_paths(self) -> list[pathlib.Path]:
        """Get source locations as Path objects"""
        return [pathlib.Path(p) for p in self.source_locations]

    def get_target_path(self) -> Optional[pathlib.Path]:
        """Get target location as Path object"""
        return pathlib.Path(self.target_location) if self.target_location else None

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
            last_scan=data.get("last_scan"),
            last_consolidation=data.get("last_consolidation"),
        )

    @classmethod
    def default(cls) -> "MonosisConfig":
        """Create default configuration"""
        return cls(
            source_locations=[],
            target_location=None,
            last_scan=None,
            last_consolidation=None,
        )


class ConfigManager:
    """Manages loading and saving configuration"""

    def __init__(self, config_dir: pathlib.Path):
        self.config_dir = config_dir
        self.config_file = config_dir / "config.json"
        self.config_dir.mkdir(exist_ok=True)

    def load(self) -> MonosisConfig:
        """Load configuration from file"""
        if self.config_file.exists():
            try:
                with self.config_file.open() as f:
                    data = json.load(f)
                    return MonosisConfig.from_dict(data)
            except (json.JSONDecodeError, KeyError):
                # If config is corrupted, return default
                return MonosisConfig.default()
        return MonosisConfig.default()

    def save(self, config: MonosisConfig):
        """Save configuration to file"""
        with self.config_file.open("w") as f:
            json.dump(config.to_dict(), f, indent=2)

    def reset(self):
        """Reset configuration to default"""
        if self.config_file.exists():
            self.config_file.unlink()
