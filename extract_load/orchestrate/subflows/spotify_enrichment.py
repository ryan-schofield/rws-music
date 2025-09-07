#!/usr/bin/env python3
"""
Spotify Enrichment Subflow.

This module contains the Prefect subflow for enriching Spotify data.
"""

import sys
from pathlib import Path
from typing import Dict, Any, Optional

from prefect import flow, get_run_logger
from prefect.states import Failed

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from extract_load.orchestrate.flow_config import FlowConfig, get_flow_config
from extract_load.orchestrate.atomic_tasks import (
    spotify_artist_enrichment,
    spotify_album_enrichment,
    spotify_mbid_update,
)
from extract_load.orchestrate.subflows.utils import check_task_success, log_subflow_summary


# SPOTIFY ENRICHMENT SUBFLOW


@flow(
    name="Spotify Enrichment Subflow",
    description="Parallel Spotify enrichment with dependency management",
    version="1.0.0",
)
def spotify_enrichment_subflow(config: Optional[FlowConfig] = None) -> Dict[str, Any]:
    """
    Spotify enrichment subflow with parallel execution.

    Concurrency Pattern:
    - Artist enrichment and Album enrichment run in parallel
    - MBID updates run after both complete (requires MBZ data)

    Args:
        config: Flow configuration (uses default if None)

    Returns:
        Dict containing results from all tasks
    """
    logger = get_run_logger()
    config = config or get_flow_config()

    logger.info("Starting Spotify enrichment subflow")

    # Step 1: Run artist and album enrichment
    logger.info("Running Spotify enrichment (artists + albums)...")

    artist_result = spotify_artist_enrichment(config, limit=config.spotify_artist_limit)
    album_result = spotify_album_enrichment(config, limit=config.spotify_album_limit)

    # Step 2: Update MBIDs (depends on MusicBrainz data being available)
    logger.info("Updating artist MBIDs...")
    mbid_result = spotify_mbid_update(config)

    # Collect results
    results = {
        "artist_enrichment": artist_result,
        "album_enrichment": album_result,
        "mbid_update": mbid_result,
    }

    # Log summary
    log_subflow_summary(logger, "Spotify Enrichment", results)

    # Determine overall status
    successful_tasks = [r for r in results.values() if check_task_success(r, "")]

    if len(successful_tasks) == len(results):
        overall_status = "success"
        logger.info("Spotify enrichment completed successfully")
    elif len(successful_tasks) > 0:
        overall_status = "partial_success"
        logger.warning("Spotify enrichment completed with some failures")
    else:
        overall_status = "failed"
        logger.error("Spotify enrichment failed completely")

    return {
        "subflow_name": "spotify_enrichment",
        "overall_status": overall_status,
        "task_results": results,
        "summary": {
            "tasks_completed": len(results),
            "tasks_successful": len(successful_tasks),
            "parallel_execution": True,
        },
    }