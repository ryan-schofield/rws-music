#!/usr/bin/env python3
"""
Prefect flows for the music tracking system.

This module contains concurrent, modular flows with the following features:

1. Code reduction through DRY principles
2. Performance improvement through concurrency
3. Atomic, testable components
4. Proper error handling and retry strategies
5. Dynamic configuration management
"""

import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional

from prefect import flow, get_run_logger
from prefect.context import get_run_context
from prefect.states import Failed, Completed

# Add project root to path for imports (more robust approach)
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from scripts.orchestrate.flow_config import FlowConfig, get_flow_config
from scripts.orchestrate.monitoring import metrics_collector, monitor_flow, send_alert
from scripts.orchestrate.atomic_tasks import spotify_api_ingestion
from scripts.orchestrate.subflows import (
    data_preparation_subflow,
    enrichment_coordination_subflow,
    transformation_subflow,
)


# SPOTIFY INGESTION FLOW


@flow(
    name="Spotify Ingestion Flow",
    description="Spotify API ingestion with optimized error handling",
    version="1.0.0",
)
def spotify_ingestion_flow(
    limit: int = 50, config: Optional[FlowConfig] = None
) -> Dict[str, Any]:
    """
    Spotify ingestion flow with the following features:
    - Direct task usage (no subprocess duplication)
    - Comprehensive error handling
    - Consistent result format
    - Configurable limits

    Args:
        limit: Maximum number of tracks to fetch per run
        config: Flow configuration (uses default if None)

    Returns:
        Dict containing flow execution results
    """
    logger = get_run_logger()
    config = config or get_flow_config()

    # Override limit with config if not specified
    if limit == 50 and config.spotify_artist_limit is not None:
        limit = min(limit, config.spotify_artist_limit)

    with monitor_flow("spotify_ingestion", {"limit": limit}) as execution_id:
        logger.info(f"Starting Spotify ingestion flow (execution: {execution_id})")

        try:
            # Execute ingestion task
            ingestion_result = spotify_api_ingestion(config, limit=limit)

            # Prepare flow result
            flow_result = {
                "flow_name": "spotify_ingestion",
                "execution_id": execution_id,
                "status": ingestion_result.get("status", "unknown"),
                "task_results": {"spotify_api_ingestion": ingestion_result},
                "summary": {
                    "records_ingested": ingestion_result.get("metrics", {}).get(
                        "records_ingested", 0
                    ),
                    "limit_used": limit,
                },
            }

            if ingestion_result.get("status") in ["success", "no_updates"]:
                logger.info("Spotify ingestion flow completed successfully")
                return flow_result
            else:
                logger.error(
                    f"Spotify ingestion flow failed: {ingestion_result.get('message', 'Unknown error')}"
                )
                raise Failed(
                    message=f"Spotify ingestion failed: {ingestion_result.get('message')}",
                    result=flow_result,
                )

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


# DAILY ETL FLOW


@flow(
    name="Daily ETL Flow",
    description="Daily pipeline with concurrent execution",
    version="1.0.0",
)
def daily_etl_flow(config: Optional[FlowConfig] = None) -> Dict[str, Any]:
    """
    Daily ETL flow

    Args:
        config: Flow configuration (uses default if None)

    Returns:
        Dict containing comprehensive flow execution results
    """
    logger = get_run_logger()
    config = config or get_flow_config()

    with monitor_flow("daily_etl") as execution_id:
        logger.info(f"Starting Daily ETL Flow (execution: {execution_id})")
        logger.info(f"Environment: {config.environment}")
        logger.info(f"Configuration: {config.to_dict()}")

        # Initialize flow results structure
        flow_results = {
            "flow_name": "daily_etl",
            "execution_id": execution_id,
            "config": config.to_dict(),
            "stages": {},
            "overall_status": "running",
            "performance_metrics": {
                "start_time": datetime.now(timezone.utc).isoformat(),
                "concurrent_stages": 1,  # Enrichment coordination stage
                "total_subflows": 0,
                "total_tasks": 0,
            },
        }

        try:
            # STAGE 1: Data Preparation (Sequential)
            logger.info("=== STAGE 1: Data Preparation ===")

            data_prep_result = data_preparation_subflow(config)
            flow_results["stages"]["data_preparation"] = data_prep_result

            # Check if data preparation succeeded
            if data_prep_result.get("overall_status") not in [
                "success",
                "partial_success",
            ]:
                logger.error("Data preparation failed - stopping pipeline")
                return _finalize_flow(flow_results, "failed", config)

            logger.info("Data preparation completed successfully")

            # STAGE 2: Enrichment Coordination (Concurrent)
            logger.info("=== STAGE 2: Enrichment Coordination (Concurrent) ===")

            enrichment_result = enrichment_coordination_subflow(config)
            flow_results["stages"]["enrichment"] = enrichment_result

            # Update performance metrics
            enrichment_summary = enrichment_result.get("summary", {})
            flow_results["performance_metrics"][
                "total_subflows"
            ] = enrichment_summary.get("subflows_completed", 0)
            flow_results["performance_metrics"][
                "total_tasks"
            ] += enrichment_summary.get("total_tasks", 0)

            # Enrichment can partially succeed - continue if any subflow succeeded
            successful_subflows = enrichment_summary.get("subflows_successful", 0)
            if successful_subflows == 0:
                logger.error("All enrichment subflows failed - stopping pipeline")
                return _finalize_flow(flow_results, "failed", config)

            if successful_subflows < enrichment_summary.get("subflows_completed", 0):
                logger.warning(
                    f"Enrichment partially successful: {successful_subflows} subflows succeeded"
                )
            else:
                logger.info("Enrichment coordination completed successfully")

            # STAGE 3: Data Transformations (Sequential)
            logger.info("=== STAGE 3: Data Transformations ===")

            transformation_result = transformation_subflow(config)
            flow_results["stages"]["transformations"] = transformation_result

            # Update performance metrics
            trans_summary = transformation_result.get("summary", {})
            flow_results["performance_metrics"]["total_tasks"] += trans_summary.get(
                "tasks_completed", 0
            )

            # Check transformation results
            if transformation_result.get("overall_status") not in [
                "success",
                "partial_success",
            ]:
                logger.error("Transformations failed - pipeline completed with errors")
                return _finalize_flow(flow_results, "partial_success", config)

            logger.info("Data transformations completed successfully")

            # Pipeline Success
            logger.info("=== Daily ETL Pipeline Completed Successfully ===")

            # Determine final status based on all stages
            if (
                data_prep_result.get("overall_status") == "success"
                and enrichment_result.get("overall_status")
                in ["success", "partial_success"]
                and transformation_result.get("overall_status")
                in ["success", "partial_success"]
            ):
                final_status = "success"
            else:
                final_status = "partial_success"

            return _finalize_flow(flow_results, final_status, config)

        except Exception as e:
            error_msg = f"Daily ETL flow failed with exception: {e}"
            logger.error(error_msg, exc_info=True)

            # Send alert for critical pipeline failures
            send_alert(
                f"Daily ETL pipeline failed: {str(e)}",
                level="error",
                execution_id=execution_id,
                flow_name="daily_etl",
                stages_completed=list(flow_results.get("stages", {}).keys()),
            )

            flow_results["error"] = {
                "type": type(e).__name__,
                "message": str(e),
            }

            return _finalize_flow(flow_results, "failed", config)


def _finalize_flow(
    flow_results: Dict[str, Any], final_status: str, config: FlowConfig
) -> Dict[str, Any]:
    """
    Finalize the flow results with comprehensive metrics and status.

    Args:
        flow_results: Current flow results dictionary
        final_status: Final status to set
        config: Flow configuration

    Returns:
        Finalized flow results dictionary
    """
    logger = get_run_logger()

    # Update final status and timing
    end_time = datetime.now(timezone.utc)
    start_time_str = flow_results["performance_metrics"]["start_time"]
    start_time = datetime.fromisoformat(start_time_str)
    duration = (end_time - start_time).total_seconds()

    flow_results["overall_status"] = final_status
    flow_results["performance_metrics"].update(
        {
            "end_time": end_time.isoformat(),
            "duration_seconds": duration,
        }
    )

    # Calculate comprehensive statistics
    stages_completed = len(flow_results.get("stages", {}))
    stages_successful = sum(
        1
        for stage_result in flow_results.get("stages", {}).values()
        if stage_result.get("overall_status") in ["success", "partial_success"]
    )

    total_tasks_successful = 0
    for stage_result in flow_results.get("stages", {}).values():
        if "summary" in stage_result:
            if "total_successful_tasks" in stage_result["summary"]:
                total_tasks_successful += stage_result["summary"][
                    "total_successful_tasks"
                ]
            elif "tasks_successful" in stage_result["summary"]:
                total_tasks_successful += stage_result["summary"]["tasks_successful"]

    flow_results["summary"] = {
        "status": final_status,
        "duration_seconds": duration,
        "stages_completed": stages_completed,
        "stages_successful": stages_successful,
        "total_tasks": flow_results["performance_metrics"]["total_tasks"],
        "total_tasks_successful": total_tasks_successful,
    }

    # Log final summary
    logger.info(f"Daily ETL Flow Summary:")
    logger.info(f"  Status: {final_status}")
    logger.info(f"  Duration: {duration:.1f} seconds")
    logger.info(f"  Stages: {stages_successful}/{stages_completed} successful")
    logger.info(
        f"  Tasks: {total_tasks_successful}/{flow_results['performance_metrics']['total_tasks']} successful"
    )
    logger.info(f"  Environment: {config.environment}")

    if final_status == "success":
        logger.info("Daily ETL pipeline completed successfully with full concurrency!")
    elif final_status == "partial_success":
        logger.warning("Daily ETL pipeline completed with some issues")
    else:
        logger.error("Daily ETL pipeline failed")

    return flow_results


# UTILITY FUNCTIONS


def get_flow_run_info() -> Dict[str, Any]:
    """
    Get comprehensive information about the current flow run context.

    Returns:
        Dict containing comprehensive flow run information
    """
    try:
        context = get_run_context()
        return {
            "flow_run_id": context.flow_run.id,
            "flow_run_name": context.flow_run.name,
            "flow_name": context.flow.name,
            "flow_version": getattr(context.flow, "version", "unknown"),
            "start_time": (
                context.flow_run.start_time.isoformat()
                if context.flow_run.start_time
                else None
            ),
            "parameters": context.flow_run.parameters,
            "state": str(context.flow_run.state) if context.flow_run.state else None,
        }
    except Exception:
        # Not running in a flow context
        return {
            "flow_run_id": None,
            "flow_run_name": None,
            "flow_name": None,
            "flow_version": None,
            "start_time": None,
            "parameters": {},
            "state": None,
        }


# MAIN EXECUTION (For Testing)


if __name__ == "__main__":
    import argparse
    import json

    parser = argparse.ArgumentParser(description="Prefect Flows for Music Tracker")
    parser.add_argument(
        "--flow",
        choices=["spotify", "etl"],
        required=True,
        help="Which flow to run",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Limit for processing (default: 50)",
    )
    parser.add_argument(
        "--config-env",
        choices=["development", "testing", "production"],
        default="development",
        help="Configuration environment (default: development)",
    )

    args = parser.parse_args()

    # Set environment for configuration
    import os

    os.environ["ENVIRONMENT"] = args.config_env

    # Get configuration
    config = get_flow_config()

    print(f"Running {args.flow} flow in {config.environment} environment...")
    print(f"Configuration: {json.dumps(config.to_dict(), indent=2, default=str)}")

    try:
        if args.flow == "spotify":
            result = spotify_ingestion_flow(limit=args.limit, config=config)
        elif args.flow == "etl":
            result = daily_etl_flow(config=config)

        # Print results
        print("\n" + "=" * 80)
        print("FLOW EXECUTION RESULTS")
        print("=" * 80)
        print(json.dumps(result, indent=2, default=str))

        # Exit with appropriate code
        status = result.get("overall_status", "unknown")
        if status in ["success", "partial_success"]:
            print(f"\nFlow completed with status: {status}")
            exit(0)
        else:
            print(f"\nFlow failed with status: {status}")
            exit(1)

    except KeyboardInterrupt:
        print("\nFlow interrupted by user")
        exit(2)
    except Exception as e:
        print(f"\nFlow failed with exception: {e}")
        exit(1)
