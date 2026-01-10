"""Configuration for Streamlit app."""

import os
from pathlib import Path

# Database
DUCKDB_PATH = os.getenv(
    "DUCKDB_PATH", Path(__file__).parent.parent / "data" / "music_tracker.duckdb"
)

# App
REFRESH_CACHE_TTL = 300  # 5 minutes
TOP_ARTISTS_LIMIT = 15
DEFAULT_TIMEZONE = os.getenv("TIMEZONE", "UTC")

# UI
PAGE_TITLE = "🎵 Listening Analytics"
PAGE_ICON = "🎵"
LAYOUT = "wide"
