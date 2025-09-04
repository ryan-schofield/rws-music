#!/usr/bin/env python3
"""
Configuration management for Prefect flows.

Centralized configuration to eliminate hardcoded values and enable
dynamic parameter management across all flows and tasks.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Any, Optional


@dataclass
class FlowConfig:
    """Centralized configuration for all Prefect flows."""

    # Data paths
    data_base_path: str = "data/src"
    cache_dir: str = "data/cache"
    dbt_dir: str = "dbt"

    # Processing limits for development/testing
    spotify_artist_limit: Optional[int] = None
    spotify_album_limit: Optional[int] = None
    musicbrainz_fetch_limit: Optional[int] = None
    musicbrainz_hierarchy_limit: Optional[int] = None

    # API batch sizes
    spotify_artist_batch_size: int = 50
    spotify_album_batch_size: int = 20

    # Timeouts (seconds)
    default_timeout: int = 300
    api_timeout: int = 900
    long_running_timeout: int = 1800
    dbt_timeout: int = 1200

    # Retry configurations
    default_retries: int = 2
    api_retries: int = 3

    # Retry delays (seconds)
    default_retry_delay: int = 30
    api_retry_delay: int = 60

    # Concurrency settings
    max_concurrent_enrichment: int = 3
    max_concurrent_api_calls: int = 5

    # Environment-specific overrides
    environment: str = field(
        default_factory=lambda: os.getenv("ENVIRONMENT", "development")
    )

    def __post_init__(self):
        """Apply environment-specific configurations."""
        if self.environment == "production":
            # Production limits to prevent API rate limiting
            self.spotify_artist_limit = 200
            self.spotify_album_limit = 100
            self.musicbrainz_fetch_limit = 50
            self.musicbrainz_hierarchy_limit = 100

        elif self.environment == "testing":
            # Testing limits for faster execution
            self.spotify_artist_limit = 10
            self.spotify_album_limit = 5
            self.musicbrainz_fetch_limit = 5
            self.musicbrainz_hierarchy_limit = 10

        # Convert string paths to Path objects
        self.data_base_path = Path(self.data_base_path)
        self.cache_dir = Path(self.cache_dir)
        self.dbt_dir = Path(self.dbt_dir)

    def to_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary for logging/serialization."""
        config_dict = {}
        for key, value in self.__dict__.items():
            if isinstance(value, Path):
                config_dict[key] = str(value)
            else:
                config_dict[key] = value
        return config_dict

    @classmethod
    def from_env(cls) -> "FlowConfig":
        """Create config from environment variables."""
        config = cls()

        # Override with environment variables if they exist
        env_mappings = {
            "DATA_BASE_PATH": "data_base_path",
            "CACHE_DIR": "cache_dir",
            "DBT_DIR": "dbt_dir",
            "SPOTIFY_ARTIST_LIMIT": "spotify_artist_limit",
            "SPOTIFY_ALBUM_LIMIT": "spotify_album_limit",
            "MUSICBRAINZ_FETCH_LIMIT": "musicbrainz_fetch_limit",
            "MUSICBRAINZ_HIERARCHY_LIMIT": "musicbrainz_hierarchy_limit",
            "DEFAULT_TIMEOUT": "default_timeout",
            "API_TIMEOUT": "api_timeout",
            "DEFAULT_RETRIES": "default_retries",
            "API_RETRIES": "api_retries",
        }

        for env_var, config_attr in env_mappings.items():
            env_value = os.getenv(env_var)
            if env_value is not None:
                # Convert string values to appropriate types
                if (
                    config_attr.endswith("_limit")
                    or config_attr.endswith("_timeout")
                    or config_attr.endswith("_retries")
                ):
                    try:
                        env_value = (
                            int(env_value) if env_value.lower() != "none" else None
                        )
                    except ValueError:
                        continue
                setattr(config, config_attr, env_value)

        return config


# Global default configuration instance
default_config = FlowConfig()


def get_flow_config() -> FlowConfig:
    """Get the current flow configuration."""
    return FlowConfig.from_env()
