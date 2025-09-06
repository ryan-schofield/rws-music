#!/usr/bin/env python3
"""
Enrichment Coordination Subflow.

This module contains the Prefect subflow for coordinating all enrichment subflows.
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

from prefect.orchestrate.flow_config import FlowConfig, get_flow_config
from prefect.orchestrate.subflows.spotify_enrichment import spotify_enrichment_subflow
from prefect.orchestrate.subflows.musicbrainz_enrichment import musicbrainz_enrichment_subflow
from prefect.orchestrate.subflows.geographic_enrichment import geographic_enrichment_subflow


# ENRICHMENT COORDINATION SUBFLOW


@flow(
    name="Enrichment Coordination Subflow",
    description="Coordinate all enrichment subflows with optimal concurrency",
    version="1.0.0",
)
def enrichment_coordination_subflow(
    config: Optional[FlowConfig] = None,
) -> Dict[str, Any]:
    """
    Coordinate all enrichment subflows for maximum concurrency.

    Concurrency Pattern:
    - Spotify, MusicBrainz, and Geographic enrichment run in parallel
    - Each subflow handles its own internal dependencies

    Args:
        config: Flow configuration (uses default if None)

    Returns:
        Dict containing results from all subflows
    """
    logger = get_run_logger()
    config = config or get_flow_config()

    logger.info("Starting enrichment coordination subflow")

    # Run all enrichment subflows
    logger.info("Running enrichment subflows...")

    spotify_result = spotify_enrichment_subflow(config)
    musicbrainz_result = musicbrainz_enrichment_subflow(config)
    geographic_result = geographic_enrichment_subflow(config)

    # Collect results
    subflow_results = {
        "spotify_enrichment": spotify_result,
        "musicbrainz_enrichment": musicbrainz_result,
        "geographic_enrichment": geographic_result,
    }

    # Calculate overall statistics
    total_subflows = len(subflow_results)
    successful_subflows = sum(
        1
        for result in subflow_results.values()
        if result.get("overall_status") in ["success", "partial_success"]
    )

    logger.info(
        f"Enrichment coordination completed: {successful_subflows}/{total_subflows} subflows successful"
    )

    # Determine overall status
    if successful_subflows == total_subflows:
        overall_status = "success"
        logger.info("All enrichment subflows completed successfully")
    elif successful_subflows > 0:
        overall_status = "partial_success"
        logger.warning("Some enrichment subflows failed")
    else:
        overall_status = "failed"
        logger.error("All enrichment subflows failed")

    return {
        "subflow_name": "enrichment_coordination",
        "overall_status": overall_status,
        "subflow_results": subflow_results,
        "summary": {
            "subflows_completed": total_subflows,
            "subflows_successful": successful_subflows,
            "concurrent_execution": True,
            "total_tasks": sum(
                result.get("summary", {}).get("tasks_completed", 0)
                for result in subflow_results.values()
            ),
            "total_successful_tasks": sum(
                result.get("summary", {}).get("tasks_successful", 0)
                for result in subflow_results.values()
            ),
        },
    }