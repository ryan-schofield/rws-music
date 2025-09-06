#!/usr/bin/env python3
"""
Daily ETL Flow.

This module contains the Prefect flow for the daily ETL pipeline.
"""

import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional

from prefect import flow, get_run_logger
from prefect.states import Failed

# Add project root to path for imports (more robust approach)
project_root = Path(__file__).parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from prefect.orchestrate.flow_config import FlowConfig, get_flow_config
from prefect.orchestrate.monitoring import metrics_collector, monitor_flow, send_alert
from prefect.orchestrate.subflows import (
    data_preparation_subflow,
    enrichment_coordination_subflow,
    transformation_subflow,
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