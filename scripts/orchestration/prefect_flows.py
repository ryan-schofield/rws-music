#!/usr/bin/env python3
"""
Prefect flows for orchestrating the music tracking pipeline.

This module defines Prefect flows for scheduling and monitoring
the data pipeline execution.
"""

import os
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, Any

import prefect
from prefect import flow, task, get_run_context
from prefect.logging import get_run_logger
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=getattr(logging, os.getenv('LOG_LEVEL', 'INFO')),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@task
def run_pipeline_stage(stage_name: str, script_path: str, args: list = None) -> Dict[str, Any]:
    """Run a specific pipeline stage."""
    import subprocess
    import sys
    import json

    logger = get_run_logger()

    try:
        cmd = [sys.executable, script_path]
        if args:
            cmd.extend(args)

        logger.info(f"Running {stage_name}: {' '.join(cmd)}")

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent.parent
        )

        if result.returncode == 0:
            stage_result = json.loads(result.stdout)
            logger.info(f"{stage_name} completed successfully")
            return stage_result
        else:
            logger.error(f"{stage_name} failed: {result.stderr}")
            return {
                "status": "error",
                "stage": stage_name,
                "message": result.stderr
            }

    except Exception as e:
        logger.error(f"Error running {stage_name}: {e}")
        return {
            "status": "error",
            "stage": stage_name,
            "message": str(e)
        }


@task
def check_pipeline_health() -> Dict[str, Any]:
    """Check the health of the pipeline components."""
    logger = get_run_logger()

    try:
        # Check if DuckDB file exists and is accessible
        duckdb_path = os.getenv('DUCKDB_PATH', './data/music_tracker.duckdb')
        if not Path(duckdb_path).exists():
            return {"status": "warning", "message": "DuckDB file does not exist yet"}

        # Check if required directories exist
        required_dirs = ['./data', './logs', './data/raw']
        for dir_path in required_dirs:
            if not Path(dir_path).exists():
                Path(dir_path).mkdir(parents=True, exist_ok=True)

        logger.info("Pipeline health check passed")
        return {"status": "healthy", "message": "All components ready"}

    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {"status": "error", "message": str(e)}


@task
def send_notification(result: Dict[str, Any], webhook_url: str = None) -> None:
    """Send notification about pipeline completion."""
    logger = get_run_logger()

    try:
        status = result.get("overall_status", result.get("status", "unknown"))
        duration = result.get("duration_seconds", 0)

        message = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "pipeline_status": status,
            "duration_seconds": duration,
            "stages": result.get("stages", {})
        }

        logger.info(f"Pipeline completed with status: {status}")

        # If webhook URL is provided, send notification
        if webhook_url:
            import requests
            try:
                requests.post(webhook_url, json=message, timeout=10)
                logger.info("Notification sent to webhook")
            except Exception as e:
                logger.warning(f"Failed to send webhook notification: {e}")

    except Exception as e:
        logger.error(f"Error sending notification: {e}")


@flow(name="spotify-daily-ingestion")
def spotify_daily_ingestion_flow() -> Dict[str, Any]:
    """Daily flow for Spotify data ingestion and processing."""
    logger = get_run_logger()
    logger.info("Starting Spotify daily ingestion flow")

    # Health check
    health_result = check_pipeline_health()
    if health_result.get("status") == "error":
        logger.error("Health check failed, aborting flow")
        return {"status": "failed", "reason": "health_check_failed"}

    # Run ingestion
    ingestion_result = run_pipeline_stage(
        "spotify_ingestion",
        "./scripts/ingestion/spotify_api_ingestion.py",
        ["--limit", "50"]
    )

    if ingestion_result.get("status") != "success":
        logger.error("Ingestion failed, aborting flow")
        return {"status": "failed", "stage": "ingestion", "result": ingestion_result}

    # Run processing
    processing_result = run_pipeline_stage(
        "data_processing",
        "./scripts/processing/merge_spotify_recently_played.py"
    )

    if processing_result.get("status") != "success":
        logger.error("Processing failed")
        return {"status": "failed", "stage": "processing", "result": processing_result}

    # Run dbt transformations
    dbt_result = run_pipeline_stage(
        "dbt_transformations",
        "./scripts/run_pipeline.py",
        ["--stage", "dbt"]
    )

    if dbt_result.get("status") != "success":
        logger.error("dbt transformations failed")
        return {"status": "failed", "stage": "dbt", "result": dbt_result}

    # Success
    result = {
        "status": "success",
        "ingestion": ingestion_result,
        "processing": processing_result,
        "dbt": dbt_result
    }

    logger.info("Spotify daily ingestion flow completed successfully")
    return result


@flow(name="music-tracker-full-pipeline")
def full_pipeline_flow() -> Dict[str, Any]:
    """Complete pipeline flow including all stages."""
    logger = get_run_logger()
    logger.info("Starting full music tracker pipeline flow")

    # Health check
    health_result = check_pipeline_health()
    if health_result.get("status") == "error":
        logger.error("Health check failed, aborting flow")
        return {"status": "failed", "reason": "health_check_failed"}

    # Run full pipeline
    pipeline_result = run_pipeline_stage(
        "full_pipeline",
        "./scripts/run_pipeline.py"
    )

    if pipeline_result.get("overall_status") != "success":
        logger.error("Full pipeline failed")
        return {"status": "failed", "result": pipeline_result}

    logger.info("Full pipeline flow completed successfully")
    return pipeline_result


@flow(name="music-tracker-maintenance")
def maintenance_flow() -> Dict[str, Any]:
    """Maintenance flow for cleanup and optimization."""
    logger = get_run_logger()
    logger.info("Starting maintenance flow")

    try:
        # This could include:
        # - Database optimization
        # - Log cleanup
        # - Backup operations
        # - Health checks

        logger.info("Maintenance tasks completed")
        return {"status": "success", "message": "Maintenance completed"}

    except Exception as e:
        logger.error(f"Maintenance flow failed: {e}")
        return {"status": "failed", "message": str(e)}


# Deployment configuration for Prefect
if __name__ == "__main__":
    # Example of how to run flows locally
    import asyncio

    async def run_example():
        # Run the daily ingestion flow
        result = await spotify_daily_ingestion_flow()
        print(f"Flow result: {result}")

    # Uncomment to run locally
    # asyncio.run(run_example())
    pass