#!/usr/bin/env python3
"""
Simplified Prefect configuration for the music tracking system.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class PrefectConfig:
    """Essential configuration settings for Prefect orchestration."""

    # Project settings
    PROJECT_NAME = "music-tracker"
    PROJECT_DESCRIPTION = "Open-source music tracking and analytics platform"

    # Environment settings
    ENVIRONMENT = os.getenv("PREFECT_ENV", "development")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

    # Database settings
    DUCKDB_PATH = os.getenv("DUCKDB_PATH", "./data/music_tracker.duckdb")

    # Spotify API settings
    SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
    SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
    SPOTIFY_REFRESH_TOKEN = os.getenv("SPOTIFY_REFRESH_TOKEN")

    # Scheduling settings
    SPOTIFY_INGESTION_INTERVAL_MINUTES = 10
    DAILY_ETL_HOUR = 2  # Run daily ETL at 2 AM UTC

    @classmethod
    def get_database_url(cls) -> str:
        """Get the DuckDB database URL."""
        return f"duckdb:///{cls.DUCKDB_PATH}"

    @classmethod
    def get_environment_variables(cls) -> dict:
        """Get environment variables for flow execution."""
        return {
            "SPOTIFY_CLIENT_ID": cls.SPOTIFY_CLIENT_ID or "",
            "SPOTIFY_CLIENT_SECRET": cls.SPOTIFY_CLIENT_SECRET or "",
            "SPOTIFY_REFRESH_TOKEN": cls.SPOTIFY_REFRESH_TOKEN or "",
            "DUCKDB_PATH": cls.DUCKDB_PATH,
            "LOG_LEVEL": cls.LOG_LEVEL,
            "PREFECT_ENV": cls.ENVIRONMENT,
        }

    @classmethod
    def validate_configuration(cls) -> dict:
        """Validate that required configuration is present."""
        validation_results = {"valid": True, "missing": [], "warnings": []}

        # Check required Spotify credentials
        spotify_credentials = {
            "SPOTIFY_CLIENT_ID": cls.SPOTIFY_CLIENT_ID,
            "SPOTIFY_CLIENT_SECRET": cls.SPOTIFY_CLIENT_SECRET,
            "SPOTIFY_REFRESH_TOKEN": cls.SPOTIFY_REFRESH_TOKEN,
        }

        for cred_name, cred_value in spotify_credentials.items():
            if not cred_value:
                validation_results["missing"].append(cred_name)
                validation_results["valid"] = False

        # Check database path
        if not cls.DUCKDB_PATH:
            validation_results["missing"].append("DUCKDB_PATH")
            validation_results["valid"] = False

        return validation_results


if __name__ == "__main__":
    import json

    print("Prefect Configuration")
    print("=" * 30)
    print(f"Project: {PrefectConfig.PROJECT_NAME}")
    print(f"Environment: {PrefectConfig.ENVIRONMENT}")
    print(f"Database URL: {PrefectConfig.get_database_url()}")

    print("\nValidation Results:")
    validation = PrefectConfig.validate_configuration()
    print(json.dumps(validation, indent=2))
