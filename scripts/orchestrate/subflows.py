#!/usr/bin/env python3
"""
Subflow implementations for concurrent task execution.

These subflows organize atomic tasks into logical groups with proper
dependency management and concurrent execution where possible.
"""

import sys
from pathlib import Path
from typing import Dict, Any, List, Optional

from prefect import flow, get_run_logger

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from scripts.orchestrate.flow_config import FlowConfig, get_flow_config
from scripts.orchestrate.atomic_tasks import (
    # Data loading tasks
    load_raw_tracks_data,
    validate_data_quality,
    # Spotify tasks
    spotify_api_ingestion,
    spotify_artist_enrichment,
    spotify_album_enrichment,
    spotify_mbid_update,
    # MusicBrainz tasks
    musicbrainz_discovery,
    musicbrainz_fetch,
    musicbrainz_parse,
    musicbrainz_hierarchy,
    # Geographic tasks
    geographic_enrichment,
    # Transformation tasks
    dbt_transformations,
    update_reporting_data,
)


def check_task_success(task_result: Dict[str, Any], task_name: str) -> bool:
    """Check if a task completed successfully."""
    status = task_result.get("status", "unknown")
    success_statuses = ["success", "no_updates", "partial_success"]
    return status in success_statuses


def log_subflow_summary(logger, subflow_name: str, results: Dict[str, Any]) -> None:
    """Log a summary of subflow execution results."""
    total_tasks = len(results)
    successful_tasks = sum(
        1 for result in results.values() if check_task_success(result, "")
    )
    failed_tasks = total_tasks - successful_tasks

    logger.info(
        f"{subflow_name} Summary: {successful_tasks}/{total_tasks} tasks successful"
    )

    if failed_tasks > 0:
        failed_task_names = [
            name
            for name, result in results.items()
            if not check_task_success(result, "")
        ]
        logger.warning(f"Failed tasks: {', '.join(failed_task_names)}")


# DATA PREPARATION SUBFLOW


@flow(
    name="Data Preparation Subflow",
    description="Load and validate raw data with sequential dependencies",
    version="1.0.0",
)
def data_preparation_subflow(config: Optional[FlowConfig] = None) -> Dict[str, Any]:
    """
    Data preparation subflow with sequential execution.

    Flow: Load Raw Data then Validate Data Quality

    Args:
        config: Flow configuration (uses default if None)

    Returns:
        Dict containing results from all tasks
    """
    logger = get_run_logger()
    config = config or get_flow_config()

    logger.info("Starting data preparation subflow")

    # Step 1: Load raw tracks data
    logger.info("Loading raw tracks data...")
    load_result = load_raw_tracks_data(config)

    # Step 2: Validate data quality (depends on load_result)
    logger.info("Validating data quality...")
    validation_result = validate_data_quality(config)

    # Collect results
    results = {
        "load_raw_data": load_result,
        "validate_data_quality": validation_result,
    }

    # Log summary
    log_subflow_summary(logger, "Data Preparation", results)

    # Determine overall status
    if not check_task_success(load_result, "load_raw_data"):
        overall_status = "failed"
        logger.error("Data preparation failed at loading stage")
    elif not check_task_success(validation_result, "validate_data_quality"):
        overall_status = "failed"
        logger.error("Data preparation failed at validation stage")
    else:
        overall_status = "success"
        logger.info("Data preparation completed successfully")

    return {
        "subflow_name": "data_preparation",
        "overall_status": overall_status,
        "task_results": results,
        "summary": {
            "tasks_completed": len(results),
            "tasks_successful": sum(
                1 for r in results.values() if check_task_success(r, "")
            ),
        },
    }


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

    artist_result = spotify_artist_enrichment(
        config, limit=config.spotify_artist_limit
    )
    album_result = spotify_album_enrichment(
        config, limit=config.spotify_album_limit
    )

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


# MUSICBRAINZ ENRICHMENT SUBFLOW


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


# GEOGRAPHIC ENRICHMENT SUBFLOW


@flow(
    name="Geographic Enrichment Subflow",
    description="Geographic data processing with internal parallelization",
    version="1.0.0",
)
def geographic_enrichment_subflow(
    config: Optional[FlowConfig] = None,
) -> Dict[str, Any]:
    """
    Geographic enrichment subflow.

    The GeographicProcessor handles internal parallelization of coordinates
    and continent processing, so this is a single task subflow.

    Args:
        config: Flow configuration (uses default if None)

    Returns:
        Dict containing results from all tasks
    """
    logger = get_run_logger()
    config = config or get_flow_config()

    logger.info("Starting geographic enrichment subflow")

    # Run geographic enrichment (handles internal parallelization)
    geographic_result = geographic_enrichment(config)

    # Collect results
    results = {
        "geographic_enrichment": geographic_result,
    }

    # Log summary
    log_subflow_summary(logger, "Geographic Enrichment", results)

    # Determine overall status
    if check_task_success(geographic_result, "geographic_enrichment"):
        overall_status = "success"
        logger.info("Geographic enrichment completed successfully")
    else:
        overall_status = "failed"
        logger.error("Geographic enrichment failed")

    return {
        "subflow_name": "geographic_enrichment",
        "overall_status": overall_status,
        "task_results": results,
        "summary": {
            "tasks_completed": len(results),
            "tasks_successful": 1 if check_task_success(geographic_result, "") else 0,
            "internal_parallelization": True,
        },
    }


# TRANSFORMATION SUBFLOW


@flow(
    name="Transformation Subflow",
    description="Sequential data transformation and reporting",
    version="1.0.0",
)
def transformation_subflow(config: Optional[FlowConfig] = None) -> Dict[str, Any]:
    """
    Transformation subflow with sequential execution.

    Flow: DBT Transformations then Reporting Updates

    Args:
        config: Flow configuration (uses default if None)

    Returns:
        Dict containing results from all tasks
    """
    logger = get_run_logger()
    config = config or get_flow_config()

    logger.info("Starting transformation subflow")

    # Step 1: Run DBT transformations
    logger.info("Running DBT transformations...")
    dbt_result = dbt_transformations(config)

    # Step 2: Update reporting data (depends on DBT success)
    reporting_result = None
    if check_task_success(dbt_result, "dbt_transformations"):
        logger.info("Updating reporting data...")
        reporting_result = update_reporting_data(config)
    else:
        logger.warning("Skipping reporting updates due to DBT failure")
        reporting_result = {
            "status": "skipped",
            "message": "Skipped due to DBT failure",
        }

    # Collect results
    results = {
        "dbt_transformations": dbt_result,
        "reporting_updates": reporting_result,
    }

    # Log summary
    log_subflow_summary(logger, "Transformation", results)

    # Determine overall status
    if check_task_success(dbt_result, "dbt") and check_task_success(
        reporting_result, "reporting"
    ):
        overall_status = "success"
        logger.info("Transformation subflow completed successfully")
    elif check_task_success(dbt_result, "dbt"):
        overall_status = "partial_success"
        logger.warning("Transformation completed but reporting updates failed")
    else:
        overall_status = "failed"
        logger.error("Transformation subflow failed")

    return {
        "subflow_name": "transformation",
        "overall_status": overall_status,
        "task_results": results,
        "summary": {
            "tasks_completed": len(results),
            "tasks_successful": sum(
                1 for r in results.values() if check_task_success(r, "")
            ),
        },
    }


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
