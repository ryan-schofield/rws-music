#!/usr/bin/env python3
"""
Geographic Enrichment Subflow.

This module contains the geographic enrichment subflow for geographic data processing.
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

from flows.orchestrate.flow_config import FlowConfig, get_flow_config
from flows.orchestrate.atomic_tasks import geographic_enrichment
from .utils import check_task_success, log_subflow_summary


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
        # Handle Failed state
        if hasattr(geographic_result, "is_failed") and geographic_result.is_failed():
            error_msg = getattr(geographic_result, "message", "Task failed")
            logger.error(f"Geographic enrichment failed: {error_msg}")
        # Handle dictionary result
        elif isinstance(geographic_result, dict):
            error_msg = geographic_result.get("message", "Unknown error")
            logger.error(f"Geographic enrichment failed: {error_msg}")
            if "error_message" in geographic_result:
                logger.error(f"Detailed error: {geographic_result['error_message']}")
        else:
            error_msg = "Unknown error"
            logger.error(f"Geographic enrichment failed: {error_msg}")

        # Return Failed state to fail the subflow
        return Failed(message=f"Geographic enrichment failed: {error_msg}")

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
