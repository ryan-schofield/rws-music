#!/usr/bin/env python3
"""
Prefect flows for the music tracking system.

This module contains the orchestration flows for:
1. Spotify data ingestion (runs every ~10 minutes)
2. Daily ETL pipeline (load, enrich, transform)
"""

import os
import sys
import json
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, Any, Optional
import subprocess

from prefect import flow, task
from prefect.context import get_run_context
from prefect.states import Failed
from dotenv import load_dotenv

# Import monitoring
from .monitoring import metrics_collector, monitor_flow, monitor_task, send_alert

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# ============================================================================
# TASK DEFINITIONS
# ============================================================================


@task(
    name="Run Spotify Ingestion",
    description="Fetch recently played tracks from Spotify API",
    retries=3,
    retry_delay_seconds=60,
    timeout_seconds=300,
)
def run_spotify_ingestion(limit: int = 50) -> Dict[str, Any]:
    """
    Run the Spotify API ingestion script.

    Args:
        limit: Maximum number of tracks to fetch

    Returns:
        Dict containing ingestion results
    """
    logger.info("Starting Spotify ingestion task")

    try:
        # Get the project root directory
        project_root = Path(__file__).parent.parent.parent
        script_path = project_root / "scripts" / "ingest" / "spotify_api_ingestion.py"

        # Build command
        cmd = [sys.executable, str(script_path), "--limit", str(limit)]

        # Run the ingestion script
        result = subprocess.run(
            cmd, capture_output=True, text=True, cwd=project_root, timeout=300
        )

        if result.returncode == 0:
            ingestion_result = json.loads(result.stdout)
            logger.info(
                f"Spotify ingestion completed successfully: {ingestion_result.get('records_ingested', 0)} records"
            )
            return ingestion_result
        else:
            error_msg = f"Spotify ingestion failed: {result.stderr}"
            logger.error(error_msg)
            raise Exception(error_msg)

    except subprocess.TimeoutExpired:
        error_msg = "Spotify ingestion timed out after 5 minutes"
        logger.error(error_msg)
        raise Exception(error_msg)
    except Exception as e:
        logger.error(f"Error in Spotify ingestion task: {e}")
        raise


@task(
    name="Load Raw Data",
    description="Load raw JSON data into parquet files",
    retries=2,
    retry_delay_seconds=30,
    timeout_seconds=600,
)
def load_raw_data() -> Dict[str, Any]:
    """
    Run the data loading script to process raw JSON files into parquet.

    Returns:
        Dict containing loading results
    """
    logger.info("Starting raw data loading task")

    try:
        # Get the project root directory
        project_root = Path(__file__).parent.parent.parent
        script_path = project_root / "scripts" / "load" / "append_tracks.py"

        # Run the loading script
        result = subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=True,
            text=True,
            cwd=project_root,
            timeout=600,
        )

        if result.returncode == 0:
            logger.info("Raw data loading completed successfully")
            return {
                "status": "success",
                "message": "Data loading completed",
                "stdout": result.stdout,
                "stderr": result.stderr,
            }
        else:
            error_msg = f"Data loading failed: {result.stderr}"
            logger.error(error_msg)
            raise Exception(error_msg)

    except subprocess.TimeoutExpired:
        error_msg = "Data loading timed out after 10 minutes"
        logger.error(error_msg)
        raise Exception(error_msg)
    except Exception as e:
        logger.error(f"Error in data loading task: {e}")
        raise


@task(
    name="Run Enrichment Pipeline",
    description="Run data enrichment processes (Spotify, MusicBrainz, Geographic)",
    retries=1,
    retry_delay_seconds=60,
    timeout_seconds=1800,
)
def run_enrichment_pipeline() -> Dict[str, Any]:
    """
    Run the enrichment pipeline script.

    Returns:
        Dict containing enrichment results
    """
    logger.info("Starting enrichment pipeline task")

    try:
        # Get the project root directory
        project_root = Path(__file__).parent.parent.parent
        script_path = (
            project_root / "scripts" / "orchestrate" / "enrichment_pipeline.py"
        )

        # Run the enrichment pipeline
        result = subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=True,
            text=True,
            cwd=project_root,
            timeout=1800,
        )

        if result.returncode == 0:
            enrichment_result = json.loads(result.stdout)
            logger.info("Enrichment pipeline completed successfully")
            return enrichment_result
        else:
            error_msg = f"Enrichment pipeline failed: {result.stderr}"
            logger.error(error_msg)
            raise Exception(error_msg)

    except subprocess.TimeoutExpired:
        error_msg = "Enrichment pipeline timed out after 30 minutes"
        logger.error(error_msg)
        raise Exception(error_msg)
    except Exception as e:
        logger.error(f"Error in enrichment pipeline task: {e}")
        raise


@task(
    name="Run DBT Transformations",
    description="Run dbt transformations for star schema",
    retries=2,
    retry_delay_seconds=30,
    timeout_seconds=1200,
)
def run_dbt_transformations() -> Dict[str, Any]:
    """
    Run dbt build to execute all transformations.

    Returns:
        Dict containing dbt results
    """
    logger.info("Starting dbt transformations task")

    try:
        # Get the project root directory
        project_root = Path(__file__).parent.parent.parent
        dbt_dir = project_root / "dbt"

        # Ensure dbt dependencies are installed
        logger.info("Installing dbt dependencies")
        deps_result = subprocess.run(
            [
                "dbt",
                "deps",
                "--profiles-dir",
                str(dbt_dir),
                "--project-dir",
                str(dbt_dir),
            ],
            capture_output=True,
            text=True,
            cwd=dbt_dir,
            timeout=300,
        )

        if deps_result.returncode != 0:
            logger.warning(f"dbt deps failed: {deps_result.stderr}")

        # Run dbt build
        logger.info("Running dbt build")
        build_result = subprocess.run(
            [
                "dbt",
                "build",
                "--profiles-dir",
                str(dbt_dir),
                "--project-dir",
                str(dbt_dir),
            ],
            capture_output=True,
            text=True,
            cwd=dbt_dir,
            timeout=1200,
        )

        if build_result.returncode == 0:
            logger.info("dbt transformations completed successfully")
            return {
                "status": "success",
                "message": "dbt build completed",
                "stdout": build_result.stdout,
                "stderr": build_result.stderr,
            }
        else:
            error_msg = f"dbt build failed: {build_result.stderr}"
            logger.error(error_msg)
            raise Exception(error_msg)

    except subprocess.TimeoutExpired:
        error_msg = "dbt transformations timed out after 20 minutes"
        logger.error(error_msg)
        raise Exception(error_msg)
    except Exception as e:
        logger.error(f"Error in dbt transformations task: {e}")
        raise


@task(
    name="Update Reporting Data",
    description="Update Metabase datasets and refresh reports",
    retries=1,
    retry_delay_seconds=30,
    timeout_seconds=300,
)
def update_reporting_data() -> Dict[str, Any]:
    """
    Update reporting data for Metabase integration.

    Returns:
        Dict containing reporting update results
    """
    logger.info("Starting reporting data update task")

    # Placeholder for Metabase integration
    # This would typically trigger Metabase dataset refreshes
    logger.info("Reporting data update - placeholder for Metabase integration")

    return {
        "status": "success",
        "message": "Reporting data update completed (placeholder)",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ============================================================================
# FLOW DEFINITIONS
# ============================================================================


@flow(
    name="Spotify Ingestion Flow",
    description="Fetch recently played tracks from Spotify API every 10 minutes",
    version="1.0.0",
)
def spotify_ingestion_flow(limit: int = 50) -> Dict[str, Any]:
    """
    Main flow for Spotify data ingestion.

    This flow runs every ~10 minutes to fetch recently played tracks
    from the Spotify API and store them as raw JSON files.

    Args:
        limit: Maximum number of tracks to fetch per run

    Returns:
        Dict containing flow execution results
    """
    with monitor_flow("spotify_ingestion", {"limit": limit}) as execution_id:
        logger.info(f"Starting Spotify ingestion flow (execution: {execution_id})")

        try:
            # Run the ingestion task
            with monitor_task(execution_id, "spotify_ingestion"):
                ingestion_result = run_spotify_ingestion(limit=limit)

            # Calculate flow metrics
            flow_result = {
                "flow_name": "spotify_ingestion",
                "execution_id": execution_id,
                "status": "success",
                "ingestion_result": ingestion_result,
            }

            logger.info(f"Spotify ingestion flow completed successfully")
            return flow_result

        except Exception as e:
            error_msg = f"Spotify ingestion flow failed: {e}"
            logger.error(error_msg)

            # Send alert for ingestion failures
            send_alert(
                f"Spotify ingestion flow failed: {str(e)}",
                level="error",
                execution_id=execution_id,
                flow_name="spotify_ingestion",
            )

            raise Failed(
                message=error_msg,
                result={
                    "flow_name": "spotify_ingestion",
                    "execution_id": execution_id,
                    "status": "failed",
                    "error": str(e),
                },
            )


@flow(
    name="Daily ETL Flow",
    description="Daily pipeline for loading, enriching, and transforming music data",
    version="1.0.0",
)
def daily_etl_flow() -> Dict[str, Any]:
    """
    Main flow for daily ETL processing.

    This flow runs daily and includes:
    1. Loading raw data into parquet files
    2. Running enrichment processes (Spotify, MusicBrainz, Geographic)
    3. Running dbt transformations for star schema
    4. Updating reporting data

    Returns:
        Dict containing flow execution results
    """
    with monitor_flow("daily_etl") as execution_id:
        logger.info(f"Starting daily ETL flow (execution: {execution_id})")

        flow_results = {
            "flow_name": "daily_etl",
            "execution_id": execution_id,
            "stages": {},
        }

        try:
            # Stage 1: Load raw data
            logger.info("=== ETL Stage 1: Loading Raw Data ===")
            with monitor_task(execution_id, "load_raw_data"):
                load_result = load_raw_data()
            flow_results["stages"]["load"] = load_result

            # Stage 2: Run enrichment pipeline
            logger.info("=== ETL Stage 2: Running Enrichment Pipeline ===")
            with monitor_task(execution_id, "enrichment_pipeline"):
                enrichment_result = run_enrichment_pipeline()
            flow_results["stages"]["enrichment"] = enrichment_result

            # Stage 3: Run dbt transformations
            logger.info("=== ETL Stage 3: Running DBT Transformations ===")
            with monitor_task(execution_id, "dbt_transformations"):
                dbt_result = run_dbt_transformations()
            flow_results["stages"]["dbt"] = dbt_result

            # Stage 4: Update reporting data
            logger.info("=== ETL Stage 4: Updating Reporting Data ===")
            with monitor_task(execution_id, "update_reporting"):
                reporting_result = update_reporting_data()
            flow_results["stages"]["reporting"] = reporting_result

            flow_results["status"] = "success"
            logger.info("Daily ETL flow completed successfully")
            return flow_results

        except Exception as e:
            error_msg = f"Daily ETL flow failed: {e}"
            logger.error(error_msg)

            flow_results.update({"status": "failed", "error": str(e)})

            # Send alert for ETL failures
            send_alert(
                f"Daily ETL flow failed: {str(e)}",
                level="error",
                execution_id=execution_id,
                flow_name="daily_etl",
                stages_completed=list(flow_results["stages"].keys()),
            )

            raise Failed(message=error_msg, result=flow_results)


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================


def get_flow_run_info() -> Dict[str, Any]:
    """
    Get information about the current flow run context.

    Returns:
        Dict containing flow run information
    """
    try:
        context = get_run_context()
        return {
            "flow_run_id": context.flow_run.id,
            "flow_run_name": context.flow_run.name,
            "flow_name": context.flow.name,
            "start_time": context.flow_run.start_time.isoformat()
            if context.flow_run.start_time
            else None,
            "parameters": context.flow_run.parameters,
        }
    except Exception:
        # Not running in a flow context
        return {
            "flow_run_id": None,
            "flow_run_name": None,
            "flow_name": None,
            "start_time": None,
            "parameters": {},
        }


if __name__ == "__main__":
    # Allow running flows directly for testing
    import argparse

    parser = argparse.ArgumentParser(description="Prefect Flows for Music Tracker")
    parser.add_argument(
        "--flow", choices=["spotify", "etl"], required=True, help="Which flow to run"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Limit for Spotify ingestion (default: 50)",
    )

    args = parser.parse_args()

    if args.flow == "spotify":
        result = spotify_ingestion_flow(limit=args.limit)
        print(json.dumps(result, indent=2, default=str))
    elif args.flow == "etl":
        result = daily_etl_flow()
        print(json.dumps(result, indent=2, default=str))
