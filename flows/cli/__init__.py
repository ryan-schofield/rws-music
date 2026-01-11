"""
CLI module for n8n workflow orchestration.

Provides standalone Python scripts callable by n8n for all data processing tasks,
replacing Prefect's task-based approach with direct command-line invocations.
"""

from .enrich_spotify_artists_granular import (
    IdentifyMissingArtistsCLI,
    FetchArtistBatchCLI,
    WriteArtistDataCLI,
    ExtractArtistGenresCLI,
)
from .enrich_spotify_albums_granular import (
    IdentifyMissingAlbumsCLI,
    FetchAlbumBatchCLI,
    WriteAlbumDataCLI,
    ExtractAlbumGenresCLI,
)
from .enrich_mbz_artists_granular import (
    IdentifyMissingMBZArtistsCLI,
    FetchMBZArtistBatchCLI,
    TrackMBZFailuresCLI,
)
from .enrich_geography_base import EnrichGeographyBaseCLI
from .enrich_geography_coordinates_granular import (
    IdentifyCitiesNeedingCoordinatesCLI,
    FetchCoordinateBatchCLI,
    WriteCoordinateDataCLI,
)

__all__ = [
    "base",
    "utils",
    # Spotify Artist Enrichment
    "IdentifyMissingArtistsCLI",
    "FetchArtistBatchCLI",
    "WriteArtistDataCLI",
    "ExtractArtistGenresCLI",
    # Spotify Album Enrichment
    "IdentifyMissingAlbumsCLI",
    "FetchAlbumBatchCLI",
    "WriteAlbumDataCLI",
    "ExtractAlbumGenresCLI",
    # MBZ Artist Enrichment
    "IdentifyMissingMBZArtistsCLI",
    "FetchMBZArtistBatchCLI",
    "TrackMBZFailuresCLI",
    # Geography Enrichment
    "EnrichGeographyBaseCLI",
    "IdentifyCitiesNeedingCoordinatesCLI",
    "FetchCoordinateBatchCLI",
    "WriteCoordinateDataCLI",
]
