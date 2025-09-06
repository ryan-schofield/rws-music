#!/usr/bin/env python3
"""
Utility functions for Prefect subflows.
"""

import sys
from pathlib import Path
from typing import Dict, Any

from prefect.states import Failed

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))


def check_task_success(task_result, task_name: str) -> bool:
    """Check if a task completed successfully."""
    # Handle Prefect Failed states
    if hasattr(task_result, "is_failed") and task_result.is_failed():
        return False

    # Handle dictionary results
    if isinstance(task_result, dict):
        status = task_result.get("status", "unknown")
        success_statuses = [
            "success",
            "no_updates",
            "partial_success",
            "skipped",
        ]
        return status in success_statuses

    # Default to success for unknown result types
    return True


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