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

# Add the current directory to the Python path for imports
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

from prefect import flow, task, get_run_logger
from prefect.context import get_run_context
from prefect.states import Failed, Completed
from dotenv import load_dotenv

# Import monitoring
from monitoring import metrics_collector, monitor_flow, monitor_task, send_alert

# Load environment variables
load_dotenv()


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
    logger = get_run_logger()
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
            # Log any script stderr output to surface logs in Prefect
            if result.stderr.strip():
                logger.info(f"Spotify ingestion script logs:\n{result.stderr}")
            # Log stdout output to show in Prefect UI
            if result.stdout.strip():
                logger.info(f"Spotify ingestion output:\n{result.stdout}")
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
    logger = get_run_logger()
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
            # Log any script stderr output to surface logs in Prefect
            if result.stderr.strip():
                logger.info(f"Raw data loading script logs:\n{result.stderr}")
            # Log stdout output to show in Prefect UI
            if result.stdout.strip():
                logger.info(f"Raw data loading output:\n{result.stdout}")
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
    name="Run Spotify Enrichment",
    description="Run Spotify data enrichment (artists and albums)",
    retries=2,
    retry_delay_seconds=30,
    timeout_seconds=900,
)
def run_spotify_enrichment() -> Dict[str, Any]:
    """
    Run Spotify enrichment as a standalone task.

    Returns:
        Dict containing Spotify enrichment results
    """
    logger = get_run_logger()
    logger.info("Starting Spotify enrichment task")

    try:
        # Get the project root directory
        project_root = Path(__file__).parent.parent.parent
        script_path = (
            project_root / "scripts" / "orchestrate" / "enrichment_pipeline.py"
        )

        # Run Spotify enrichment
        result = subprocess.run(
            [sys.executable, str(script_path), "--processors", "spotify"],
            capture_output=True,
            text=True,
            cwd=project_root,
            timeout=900,
        )

        if result.returncode == 0:
            if result.stderr.strip():
                logger.info(f"Spotify enrichment logs:\n{result.stderr}")
            if result.stdout.strip():
                logger.info(f"Spotify enrichment output:\n{result.stdout}")
            enrichment_result = json.loads(result.stdout)
            logger.info("Spotify enrichment completed successfully")
            return enrichment_result
        else:
            error_msg = f"Spotify enrichment failed: {result.stderr}"
            logger.error(error_msg)
            raise Exception(error_msg)

    except subprocess.TimeoutExpired:
        error_msg = "Spotify enrichment timed out after 15 minutes"
        logger.error(error_msg)
        raise Exception(error_msg)
    except Exception as e:
        logger.error(f"Error in Spotify enrichment task: {e}")
        raise


@task(
    name="Run MusicBrainz Discovery",
    description="Discover artists that need MusicBrainz enrichment",
    retries=2,
    retry_delay_seconds=30,
    timeout_seconds=300,
)
def run_musicbrainz_discovery() -> Dict[str, Any]:
    """
    Run MusicBrainz artist discovery as a standalone task.

    Returns:
        Dict containing discovery results
    """
    logger = get_run_logger()
    logger.info("Starting MusicBrainz discovery task")

    try:
        # Get the project root directory
        project_root = Path(__file__).parent.parent.parent
        script_path = (
            project_root / "scripts" / "orchestrate" / "enrichment_pipeline.py"
        )

        # Run MusicBrainz discovery
        result = subprocess.run(
            [sys.executable, str(script_path), "--processors", "musicbrainz", "--mbz-step", "discover"],
            capture_output=True,
            text=True,
            cwd=project_root,
            timeout=300,
        )

        if result.returncode == 0:
            if result.stderr.strip():
                logger.info(f"MusicBrainz discovery logs:\n{result.stderr}")
            if result.stdout.strip():
                logger.info(f"MusicBrainz discovery output:\n{result.stdout}")
            discovery_result = json.loads(result.stdout)
            logger.info("MusicBrainz discovery completed successfully")
            return discovery_result
        else:
            error_msg = f"MusicBrainz discovery failed: {result.stderr}"
            logger.error(error_msg)
            raise Exception(error_msg)

    except subprocess.TimeoutExpired:
        error_msg = "MusicBrainz discovery timed out after 5 minutes"
        logger.error(error_msg)
        raise Exception(error_msg)
    except Exception as e:
        logger.error(f"Error in MusicBrainz discovery task: {e}")
        raise


@task(
    name="Run MusicBrainz Fetch",
    description="Fetch MusicBrainz data for discovered artists",
    retries=2,
    retry_delay_seconds=30,
    timeout_seconds=900,
)
def run_musicbrainz_fetch(limit: Optional[int] = None) -> Dict[str, Any]:
    """
    Run MusicBrainz artist data fetching as a standalone task.

    Args:
        limit: Maximum number of artists to fetch

    Returns:
        Dict containing fetch results
    """
    logger = get_run_logger()
    logger.info("Starting MusicBrainz fetch task")

    try:
        # Get the project root directory
        project_root = Path(__file__).parent.parent.parent
        script_path = (
            project_root / "scripts" / "orchestrate" / "enrichment_pipeline.py"
        )

        # Build command with optional limit
        cmd = [sys.executable, str(script_path), "--processors", "musicbrainz", "--mbz-step", "fetch"]
        if limit is not None:
            cmd.extend(["--limit", str(limit)])

        # Run MusicBrainz fetch
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=project_root,
            timeout=900,
        )

        if result.returncode == 0:
            if result.stderr.strip():
                logger.info(f"MusicBrainz fetch logs:\n{result.stderr}")
            if result.stdout.strip():
                logger.info(f"MusicBrainz fetch output:\n{result.stdout}")
            fetch_result = json.loads(result.stdout)
            logger.info("MusicBrainz fetch completed successfully")
            return fetch_result
        else:
            error_msg = f"MusicBrainz fetch failed: {result.stderr}"
            logger.error(error_msg)
            raise Exception(error_msg)

    except subprocess.TimeoutExpired:
        error_msg = "MusicBrainz fetch timed out after 15 minutes"
        logger.error(error_msg)
        raise Exception(error_msg)
    except Exception as e:
        logger.error(f"Error in MusicBrainz fetch task: {e}")
        raise


@task(
    name="Run MusicBrainz Parse",
    description="Parse MusicBrainz JSON files into structured data",
    retries=2,
    retry_delay_seconds=30,
    timeout_seconds=600,
)
def run_musicbrainz_parse() -> Dict[str, Any]:
    """
    Run MusicBrainz JSON parsing as a standalone task.

    Returns:
        Dict containing parse results
    """
    logger = get_run_logger()
    logger.info("Starting MusicBrainz parse task")

    try:
        # Get the project root directory
        project_root = Path(__file__).parent.parent.parent
        script_path = (
            project_root / "scripts" / "orchestrate" / "enrichment_pipeline.py"
        )

        # Run MusicBrainz parse
        result = subprocess.run(
            [sys.executable, str(script_path), "--processors", "musicbrainz", "--mbz-step", "parse"],
            capture_output=True,
            text=True,
            cwd=project_root,
            timeout=600,
        )

        if result.returncode == 0:
            if result.stderr.strip():
                logger.info(f"MusicBrainz parse logs:\n{result.stderr}")
            if result.stdout.strip():
                logger.info(f"MusicBrainz parse output:\n{result.stdout}")
            parse_result = json.loads(result.stdout)
            logger.info("MusicBrainz parse completed successfully")
            return parse_result
        else:
            error_msg = f"MusicBrainz parse failed: {result.stderr}"
            logger.error(error_msg)
            raise Exception(error_msg)

    except subprocess.TimeoutExpired:
        error_msg = "MusicBrainz parse timed out after 10 minutes"
        logger.error(error_msg)
        raise Exception(error_msg)
    except Exception as e:
        logger.error(f"Error in MusicBrainz parse task: {e}")
        raise


@task(
    name="Run MusicBrainz Hierarchy",
    description="Process MusicBrainz area hierarchy data",
    retries=2,
    retry_delay_seconds=30,
    timeout_seconds=1200,
)
def run_musicbrainz_hierarchy(limit: Optional[int] = None) -> Dict[str, Any]:
    """
    Run MusicBrainz area hierarchy processing as a standalone task.

    Args:
        limit: Maximum number of areas to process

    Returns:
        Dict containing hierarchy results
    """
    logger = get_run_logger()
    logger.info("Starting MusicBrainz hierarchy task")

    try:
        # Get the project root directory
        project_root = Path(__file__).parent.parent.parent
        script_path = (
            project_root / "scripts" / "orchestrate" / "enrichment_pipeline.py"
        )

        # Build command with optional limit
        cmd = [sys.executable, str(script_path), "--processors", "musicbrainz", "--mbz-step", "hierarchy"]
        if limit is not None:
            cmd.extend(["--limit", str(limit)])

        # Run MusicBrainz hierarchy
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=project_root,
            timeout=1200,
        )

        if result.returncode == 0:
            if result.stderr.strip():
                logger.info(f"MusicBrainz hierarchy logs:\n{result.stderr}")
            if result.stdout.strip():
                logger.info(f"MusicBrainz hierarchy output:\n{result.stdout}")
            hierarchy_result = json.loads(result.stdout)
            logger.info("MusicBrainz hierarchy completed successfully")
            return hierarchy_result
        else:
            error_msg = f"MusicBrainz hierarchy failed: {result.stderr}"
            logger.error(error_msg)
            raise Exception(error_msg)

    except subprocess.TimeoutExpired:
        error_msg = "MusicBrainz hierarchy timed out after 20 minutes"
        logger.error(error_msg)
        raise Exception(error_msg)
    except Exception as e:
        logger.error(f"Error in MusicBrainz hierarchy task: {e}")
        raise




@task(
    name="Run Geographic Enrichment",
    description="Run geographic data enrichment (continents and coordinates)",
    retries=2,
    retry_delay_seconds=30,
    timeout_seconds=600,
)
def run_geographic_enrichment() -> Dict[str, Any]:
    """
    Run Geographic enrichment as a standalone task.

    Returns:
        Dict containing Geographic enrichment results
    """
    logger = get_run_logger()
    logger.info("Starting Geographic enrichment task")

    try:
        # Get the project root directory
        project_root = Path(__file__).parent.parent.parent
        script_path = (
            project_root / "scripts" / "orchestrate" / "enrichment_pipeline.py"
        )

        # Run Geographic enrichment
        result = subprocess.run(
            [sys.executable, str(script_path), "--processors", "geographic"],
            capture_output=True,
            text=True,
            cwd=project_root,
            timeout=600,
        )

        if result.returncode == 0:
            if result.stderr.strip():
                logger.info(f"Geographic enrichment logs:\n{result.stderr}")
            if result.stdout.strip():
                logger.info(f"Geographic enrichment output:\n{result.stdout}")
            enrichment_result = json.loads(result.stdout)
            logger.info("Geographic enrichment completed successfully")
            return enrichment_result
        else:
            error_msg = f"Geographic enrichment failed: {result.stderr}"
            logger.error(error_msg)
            raise Exception(error_msg)

    except subprocess.TimeoutExpired:
        error_msg = "Geographic enrichment timed out after 10 minutes"
        logger.error(error_msg)
        raise Exception(error_msg)
    except Exception as e:
        logger.error(f"Error in Geographic enrichment task: {e}")
        raise


@task(
    name="Run Full Enrichment Pipeline",
    description="Run complete data enrichment processes (Spotify, MusicBrainz, Geographic)",
    retries=1,
    retry_delay_seconds=60,
    timeout_seconds=1800,
)
def run_enrichment_pipeline() -> Dict[str, Any]:
    """
    Run the full enrichment pipeline script.

    Returns:
        Dict containing enrichment results
    """
    logger = get_run_logger()
    logger.info("Starting full enrichment pipeline task")

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
            # Log any script stderr output to surface logs in Prefect
            if result.stderr.strip():
                logger.info(f"Enrichment pipeline script logs:\n{result.stderr}")
            # Log stdout output to show in Prefect UI
            if result.stdout.strip():
                logger.info(f"Enrichment pipeline output:\n{result.stdout}")
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
    logger = get_run_logger()
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
        else:
            # Log any script stderr output to surface logs in Prefect
            if deps_result.stderr.strip():
                logger.info(f"dbt deps script logs:\n{deps_result.stderr}")

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
            # Log any script stderr output to surface logs in Prefect
            if build_result.stderr.strip():
                logger.info(f"dbt build script logs:\n{build_result.stderr}")
            # Log stdout output to show in Prefect UI
            if build_result.stdout.strip():
                logger.info(f"dbt build output:\n{build_result.stdout}")
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
    logger = get_run_logger()
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
    logger = get_run_logger()

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
    logger = get_run_logger()

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

            # Stage 2: Run enrichment processes individually for better control
            logger.info("=== ETL Stage 2: Enrichment Processes ===")

            enrichment_results = {}

            # Run Spotify enrichment
            logger.info("--- Running Spotify Enrichment ---")
            with monitor_task(execution_id, "spotify_enrichment"):
                spotify_result = run_spotify_enrichment()
            enrichment_results["spotify"] = spotify_result

            # Run MusicBrainz enrichment steps individually
            logger.info("--- Running MusicBrainz Enrichment ---")
            mbz_results = {}

            # MusicBrainz Discovery
            logger.info("---- MBZ Discovery ----")
            with monitor_task(execution_id, "mbz_discovery"):
                mbz_results["discovery"] = run_musicbrainz_discovery()

            # MusicBrainz Fetch (with reasonable limit for production)
            logger.info("---- MBZ Fetch ----")
            with monitor_task(execution_id, "mbz_fetch"):
                mbz_results["fetch"] = run_musicbrainz_fetch(limit=50)  # Limit to 50 artists in production

            # MusicBrainz Parse
            logger.info("---- MBZ Parse ----")
            with monitor_task(execution_id, "mbz_parse"):
                mbz_results["parse"] = run_musicbrainz_parse()

            # MusicBrainz Hierarchy (with reasonable limit for production)
            logger.info("---- MBZ Hierarchy ----")
            with monitor_task(execution_id, "mbz_hierarchy"):
                mbz_results["hierarchy"] = run_musicbrainz_hierarchy(limit=100)  # Limit to 100 areas in production

            enrichment_results["musicbrainz"] = mbz_results

            # Run Geographic enrichment
            logger.info("--- Running Geographic Enrichment ---")
            with monitor_task(execution_id, "geographic_enrichment"):
                geo_result = run_geographic_enrichment()
            enrichment_results["geographic"] = geo_result

            flow_results["stages"]["enrichment"] = enrichment_results

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
        "--flow", choices=["spotify", "etl", "mbz"], required=True, help="Which flow to run"
    )
    parser.add_argument(
        "--mbz-step",
        choices=["discover", "fetch", "parse", "hierarchy"],
        help="Specific MusicBrainz step to run (requires --flow mbz)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Limit for processing (default: 50)",
    )

    args = parser.parse_args()

    if args.flow == "spotify":
        result = spotify_ingestion_flow(limit=args.limit)
        print(json.dumps(result, indent=2, default=str))
    elif args.flow == "etl":
        result = daily_etl_flow()
        print(json.dumps(result, indent=2, default=str))
    elif args.flow == "mbz":
        if not args.mbz_step:
            print("Error: --mbz-step is required when using --flow mbz")
            print("Available steps: discover, fetch, parse, hierarchy")
            exit(1)

        if args.mbz_step == "discover":
            result = run_musicbrainz_discovery()
            print(json.dumps(result, indent=2, default=str))
        elif args.mbz_step == "fetch":
            result = run_musicbrainz_fetch(limit=args.limit)
            print(json.dumps(result, indent=2, default=str))
        elif args.mbz_step == "parse":
            result = run_musicbrainz_parse()
            print(json.dumps(result, indent=2, default=str))
        elif args.mbz_step == "hierarchy":
            result = run_musicbrainz_hierarchy(limit=args.limit)
            print(json.dumps(result, indent=2, default=str))
        else:
            print(f"Error: Unknown mbz-step '{args.mbz_step}'")
            print("Available steps: discover, fetch, parse, hierarchy")
            exit(1)
