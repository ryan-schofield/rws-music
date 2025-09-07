#!/usr/bin/env python3
"""
Data Preparation Subflow.

This module contains the Prefect subflow for loading and validating raw data.
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
    load_raw_tracks_data,
    validate_data_quality,
)
from extract_load.orchestrate.subflows.utils import check_task_success, log_subflow_summary


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