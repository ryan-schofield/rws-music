#!/usr/bin/env python3
"""
Transformation Subflow.

This module contains the transformation subflow for data transformation and reporting.
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
from flows.orchestrate.atomic_tasks import dbt_transformations, update_reporting_data
from .utils import check_task_success, log_subflow_summary


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