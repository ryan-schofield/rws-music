#!/usr/bin/env python3
"""
MusicBrainz Enrichment Subflow.

This module contains the MusicBrainz enrichment subflow for sequential enrichment tasks.
"""

import sys
from pathlib import Path
from typing import Dict, Any, Optional

from prefect import flow, get_run_logger

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from flows.orchestrate.flow_config import FlowConfig, get_flow_config
from flows.orchestrate.atomic_tasks import (
    musicbrainz_discovery,
    musicbrainz_fetch,
    musicbrainz_parse,
    musicbrainz_hierarchy,
)
from .utils import check_task_success, log_subflow_summary


@flow(
    name="MusicBrainz Enrichment Subflow",
    description="Sequential MusicBrainz enrichment with optimized dependencies",
    version="1.0.0",
)
def musicbrainz_enrichment_subflow(
    config: Optional[FlowConfig] = None,
) -> Dict[str, Any]:
    """
    MusicBrainz enrichment subflow with sequential dependencies.

    Dependency Chain:
    - Discovery then Fetch then Parse
    - Hierarchy processing (can run after Parse, parallel to other tasks)

    Args:
        config: Flow configuration (uses default if None)

    Returns:
        Dict containing results from all tasks
    """
    logger = get_run_logger()
    config = config or get_flow_config()

    logger.info("Starting MusicBrainz enrichment subflow")

    # Step 1: Discover missing artists
    logger.info("Discovering artists needing MusicBrainz enrichment...")
    discovery_result = musicbrainz_discovery(config)

    # Step 2: Fetch artist data (depends on discovery)
    fetch_result = None
    if check_task_success(discovery_result, "discovery"):
        logger.info("Fetching MusicBrainz artist data...")
        fetch_result = musicbrainz_fetch(config, limit=config.musicbrainz_fetch_limit)
    else:
        logger.warning("Skipping fetch due to discovery failure")
        fetch_result = {
            "status": "skipped",
            "message": "Skipped due to discovery failure",
        }

    # Step 3: Parse JSON files (depends on fetch, or processes existing files)
    logger.info("Parsing MusicBrainz JSON files...")
    parse_result = musicbrainz_parse(config)

    # Step 4: Process area hierarchy
    logger.info("Processing MusicBrainz area hierarchy...")
    hierarchy_result = musicbrainz_hierarchy(
        config, limit=config.musicbrainz_hierarchy_limit
    )

    # Collect results
    results = {
        "discovery": discovery_result,
        "fetch": fetch_result,
        "parse": parse_result,
        "hierarchy": hierarchy_result,
    }

    # Log summary
    log_subflow_summary(logger, "MusicBrainz Enrichment", results)

    # Determine overall status
    successful_tasks = [r for r in results.values() if check_task_success(r, "")]

    if len(successful_tasks) >= 3:  # At least discovery, parse, and hierarchy
        overall_status = "success"
        logger.info("MusicBrainz enrichment completed successfully")
    elif len(successful_tasks) >= 2:  # At least some critical tasks
        overall_status = "partial_success"
        logger.warning("MusicBrainz enrichment completed with some failures")
    else:
        overall_status = "failed"
        logger.error("MusicBrainz enrichment failed")

    return {
        "subflow_name": "musicbrainz_enrichment",
        "overall_status": overall_status,
        "task_results": results,
        "summary": {
            "tasks_completed": len(results),
            "tasks_successful": len(successful_tasks),
            "sequential_dependencies": True,
        },
    }
