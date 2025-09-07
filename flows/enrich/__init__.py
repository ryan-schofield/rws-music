"""
Data enrichment processors for rws-music.

This package consolidates the Microsoft Fabric notebooks into modular processors
that use Polars/DuckDB instead of Spark and write to parquet files instead of
Lakehouse tables.

Modules:
- geo_processor: Geographic data enrichment (continent, lat/long)
- musicbrainz_processor: MusicBrainz artist and area data enrichment
- spotify_processor: Spotify artist and album data enrichment
- utils: Shared utilities (API clients, data writers, Polars operations)
"""

from .geo_processor import GeographicProcessor
from .musicbrainz_processor import MusicBrainzProcessor
from .spotify_processor import SpotifyProcessor
from .utils.data_writer import ParquetDataWriter, EnrichmentTracker
from .utils.api_clients import SpotifyAPIClient, MusicBrainzClient, OpenWeatherGeoClient

__version__ = "1.0.0"

__all__ = [
    "GeographicProcessor",
    "MusicBrainzProcessor",
    "SpotifyProcessor",
    "ParquetDataWriter",
    "EnrichmentTracker",
    "SpotifyAPIClient",
    "MusicBrainzClient",
    "OpenWeatherGeoClient",
]
