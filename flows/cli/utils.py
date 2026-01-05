"""
Utility functions for CLI commands.
"""

import logging
from typing import Dict, Any, Optional
from pathlib import Path


logger = logging.getLogger(__name__)


def validate_environment_variables(required_vars: list) -> bool:
    """
    Validate that required environment variables are set.

    Args:
        required_vars: List of required environment variable names

    Returns:
        True if all variables are set, False otherwise
    """
    import os

    missing = []
    for var in required_vars:
        if not os.getenv(var):
            missing.append(var)

    if missing:
        logger.error(f"Missing environment variables: {', '.join(missing)}")
        return False

    return True


def validate_data_paths(paths: Dict[str, str]) -> bool:
    """
    Validate that required data paths exist.

    Args:
        paths: Dict mapping path names to path strings

    Returns:
        True if all paths exist, False otherwise
    """
    missing = []
    for name, path_str in paths.items():
        path = Path(path_str)
        if not path.exists():
            missing.append(f"{name} ({path_str})")

    if missing:
        logger.error(f"Missing required paths: {', '.join(missing)}")
        return False

    return True


def format_metrics(metrics: Dict[str, Any]) -> Dict[str, Any]:
    """
    Format metrics for JSON output.

    Args:
        metrics: Raw metrics dictionary

    Returns:
        Formatted metrics dictionary
    """
    formatted = {}
    for key, value in metrics.items():
        if isinstance(value, (int, float, str, bool, type(None))):
            formatted[key] = value
        elif isinstance(value, dict):
            formatted[key] = format_metrics(value)
        elif isinstance(value, list):
            formatted[key] = [
                item
                if isinstance(item, (int, float, str, bool, type(None)))
                else str(item)
                for item in value
            ]
        else:
            formatted[key] = str(value)

    return formatted
