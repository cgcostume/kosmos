#!/usr/bin/env python3
"""
Kosmos Configuration Manager

Unified configuration management for all kosmos tools.
Stores tool-specific configurations in a shared .kosmos directory.
"""

import json
import os
import pathlib
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional


@dataclass
class KosmosConfig:
    """Main configuration container for all kosmos tools"""

    version: str = "1.0"
    monosis: dict = field(default_factory=dict)
    photochronos: dict = field(default_factory=dict)
    # Future tools can be added here

    def get_tool_config(self, tool_name: str) -> dict:
        """Get configuration for a specific tool"""
        return getattr(self, tool_name, {})

    def set_tool_config(self, tool_name: str, config: dict):
        """Set configuration for a specific tool"""
        setattr(self, tool_name, config)

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization"""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "KosmosConfig":
        """Create from dictionary"""
        return cls(
            version=data.get("version", "1.0"),
            monosis=data.get("monosis", {}),
            photochronos=data.get("photochronos", {}),
        )


class SharedConfigManager:
    """Manages shared configuration for all kosmos tools"""

    def __init__(self, kosmos_dir: Optional[pathlib.Path] = None):
        """Initialize configuration manager

        Args:
            kosmos_dir: Override default .kosmos directory location
        """
        if kosmos_dir:
            self.kosmos_dir = kosmos_dir
        else:
            self.kosmos_dir = pathlib.Path.home() / ".kosmos"

        self.config_file = self.kosmos_dir / "config.json"
        self.cache_db = self.kosmos_dir / "hash_cache.db"

        # Ensure directory exists
        self.kosmos_dir.mkdir(exist_ok=True)

    def load(self) -> KosmosConfig:
        """Load configuration from file"""
        if self.config_file.exists():
            try:
                with self.config_file.open() as f:
                    data = json.load(f)
                    return KosmosConfig.from_dict(data)
            except (json.JSONDecodeError, KeyError):
                # If config is corrupted, return default
                return KosmosConfig()
        else:
            # Return default configuration
            return KosmosConfig()

    def save(self, config: KosmosConfig):
        """Save configuration to file"""
        with self.config_file.open("w") as f:
            json.dump(config.to_dict(), f, indent=2)

    def get_cache_db_path(self) -> pathlib.Path:
        """Get path to shared hash cache database"""
        return self.cache_db

    def migrate_from_monosis(self):
        """Migrate configuration from old .monosis directory if exists"""
        old_monosis_dir = pathlib.Path.home() / ".monosis"
        if not old_monosis_dir.exists():
            return

        # Check for old config file
        old_config_file = old_monosis_dir / "config.json"
        if old_config_file.exists():
            try:
                with old_config_file.open() as f:
                    old_data = json.load(f)

                # Create new config with monosis data
                config = self.load()
                config.monosis = old_data
                self.save(config)

                print("✓ Migrated monosis configuration to .kosmos")
            except (json.JSONDecodeError, OSError):
                print("⚠ Could not migrate monosis configuration")

    def migrate_from_photochronos(self):
        """Migrate any photochronos-specific configuration if exists"""
        # Photochronos doesn't seem to have a config file, but we can add defaults
        config = self.load()
        if not config.photochronos:
            config.photochronos = {
                "hash_algorithm": "xxhash64",  # Upgrade from md5
                "chunk_size": 65536,
                "last_run": None,
            }
            self.save(config)


def init_shared_cache_db(db_path: pathlib.Path):
    """Initialize shared hash cache database with tables for all tools"""
    import sqlite3

    with sqlite3.connect(db_path) as conn:
        # Create unified hash table (can be used by all tools)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS file_hashes (
                file_path TEXT PRIMARY KEY,
                file_size INTEGER,
                mtime REAL,
                quick_hash TEXT,
                full_hash TEXT,
                hash_algorithm TEXT,
                tool_name TEXT,
                last_scan REAL
            )
        """
        )

        # Create indices for better performance
        conn.execute("CREATE INDEX IF NOT EXISTS idx_full_hash ON file_hashes(full_hash)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_size ON file_hashes(file_size)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_tool ON file_hashes(tool_name)")

        # Tool-specific tables can be added here
        # For example, photochronos might need an operations log
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS photochronos_operations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp REAL,
                operation_type TEXT,
                source_path TEXT,
                dest_path TEXT,
                status TEXT
            )
        """
        )

        conn.commit()