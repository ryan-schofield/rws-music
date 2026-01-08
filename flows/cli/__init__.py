"""
CLI module for n8n workflow orchestration.

Provides standalone Python scripts callable by n8n for all data processing tasks,
replacing Prefect's task-based approach with direct command-line invocations.
"""

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
    "IdentifyMissingMBZArtistsCLI",
    "FetchMBZArtistBatchCLI",
    "TrackMBZFailuresCLI",
    "EnrichGeographyBaseCLI",
    "IdentifyCitiesNeedingCoordinatesCLI",
    "FetchCoordinateBatchCLI",
    "WriteCoordinateDataCLI",
]
