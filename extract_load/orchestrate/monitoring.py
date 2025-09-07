#!/usr/bin/env python3
"""
Monitoring and logging integration for Prefect flows.

This module provides:
1. Enhanced logging with structured output
2. Metrics collection for flow performance
3. Alerting capabilities for flow failures
4. Health check endpoints
"""

import os
import sys
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List
from contextlib import contextmanager

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Ensure logs directory exists
logs_dir = project_root / "logs"
logs_dir.mkdir(exist_ok=True)

# Configure structured logging
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(logs_dir / "prefect_monitoring.log"),
    ],
)
logger = logging.getLogger(__name__)


class FlowMetrics:
    """Collect and report metrics for flow execution."""

    def __init__(self):
        self.metrics_file = project_root / "logs" / "flow_metrics.json"
        self.metrics_file.parent.mkdir(exist_ok=True)
        self.current_metrics = {}

    def start_flow(self, flow_name: str, parameters: Dict[str, Any] = None) -> str:
        """Start tracking metrics for a flow execution."""
        execution_id = f"{flow_name}_{int(time.time())}"

        self.current_metrics[execution_id] = {
            "flow_name": flow_name,
            "start_time": datetime.now(timezone.utc).isoformat(),
            "parameters": parameters or {},
            "status": "running",
            "tasks": {},
            "errors": [],
        }

        logger.info(
            f"Started monitoring flow: {flow_name}",
            extra={
                "execution_id": execution_id,
                "flow_name": flow_name,
                "event": "flow_started",
            },
        )

        return execution_id

    def record_task_start(self, execution_id: str, task_name: str) -> None:
        """Record the start of a task execution."""
        if execution_id not in self.current_metrics:
            return

        self.current_metrics[execution_id]["tasks"][task_name] = {
            "start_time": datetime.now(timezone.utc).isoformat(),
            "status": "running",
            "duration": None,
            "error": None,
        }

        logger.info(
            f"Task started: {task_name}",
            extra={
                "execution_id": execution_id,
                "task_name": task_name,
                "event": "task_started",
            },
        )

    def record_task_end(
        self, execution_id: str, task_name: str, status: str, error: str = None
    ) -> None:
        """Record the end of a task execution."""
        if execution_id not in self.current_metrics:
            return

        task_info = self.current_metrics[execution_id]["tasks"].get(task_name, {})
        start_time = task_info.get("start_time")

        if start_time:
            start_dt = datetime.fromisoformat(start_time)
            duration = (datetime.now(timezone.utc) - start_dt).total_seconds()
        else:
            duration = None

        task_info.update(
            {
                "end_time": datetime.now(timezone.utc).isoformat(),
                "status": status,
                "duration": duration,
                "error": error,
            }
        )

        log_level = logging.ERROR if status == "failed" else logging.INFO
        logger.log(
            log_level,
            f"Task completed: {task_name} ({status})",
            extra={
                "execution_id": execution_id,
                "task_name": task_name,
                "status": status,
                "duration": duration,
                "error": error,
                "event": "task_completed",
            },
        )

    def end_flow(self, execution_id: str, status: str, result: Any = None) -> None:
        """End tracking metrics for a flow execution."""
        if execution_id not in self.current_metrics:
            return

        flow_metrics = self.current_metrics[execution_id]
        start_time = flow_metrics["start_time"]
        start_dt = datetime.fromisoformat(start_time)
        duration = (datetime.now(timezone.utc) - start_dt).total_seconds()

        flow_metrics.update(
            {
                "end_time": datetime.now(timezone.utc).isoformat(),
                "duration": duration,
                "status": status,
                "result": result,
            }
        )

        # Save metrics to file
        self._save_metrics()

        log_level = logging.ERROR if status == "failed" else logging.INFO
        logger.log(
            log_level,
            f"Flow completed: {flow_metrics['flow_name']} ({status})",
            extra={
                "execution_id": execution_id,
                "flow_name": flow_metrics["flow_name"],
                "status": status,
                "duration": duration,
                "event": "flow_completed",
            },
        )

        # Clean up
        del self.current_metrics[execution_id]

    def record_error(self, execution_id: str, error: str, context: str = None) -> None:
        """Record an error during flow execution."""
        if execution_id not in self.current_metrics:
            return

        error_info = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "error": error,
            "context": context,
        }

        self.current_metrics[execution_id]["errors"].append(error_info)

        logger.error(
            f"Flow error: {error}",
            extra={
                "execution_id": execution_id,
                "error": error,
                "context": context,
                "event": "flow_error",
            },
        )

    def _save_metrics(self) -> None:
        """Save current metrics to file."""
        try:
            # Load existing metrics
            existing_metrics = {}
            if self.metrics_file.exists():
                with open(self.metrics_file, "r") as f:
                    existing_metrics = json.load(f)

            # Merge with current metrics
            existing_metrics.update(self.current_metrics)

            # Save back to file
            with open(self.metrics_file, "w") as f:
                json.dump(existing_metrics, f, indent=2, default=str)

        except Exception as e:
            logger.error(f"Failed to save metrics: {e}")

    def get_flow_stats(self, flow_name: str = None, hours: int = 24) -> Dict[str, Any]:
        """Get statistics for flow executions."""
        try:
            if not self.metrics_file.exists():
                return {"error": "No metrics file found"}

            with open(self.metrics_file, "r") as f:
                all_metrics = json.load(f)

            # Filter by time and flow name
            cutoff_time = datetime.now(timezone.utc).timestamp() - (hours * 3600)
            filtered_metrics = {}

            for execution_id, metrics in all_metrics.items():
                if flow_name and metrics.get("flow_name") != flow_name:
                    continue

                start_time = datetime.fromisoformat(metrics["start_time"]).timestamp()
                if start_time >= cutoff_time:
                    filtered_metrics[execution_id] = metrics

            # Calculate statistics
            stats = {
                "total_executions": len(filtered_metrics),
                "successful_executions": 0,
                "failed_executions": 0,
                "average_duration": 0,
                "total_duration": 0,
                "executions_by_flow": {},
                "recent_executions": list(filtered_metrics.keys())[-10:],  # Last 10
            }

            total_duration = 0
            flow_counts = {}

            for execution_id, metrics in filtered_metrics.items():
                status = metrics.get("status", "unknown")
                duration = metrics.get("duration", 0)
                flow_name_exec = metrics.get("flow_name", "unknown")

                if status == "success":
                    stats["successful_executions"] += 1
                elif status in ["failed", "error"]:
                    stats["failed_executions"] += 1

                total_duration += duration

                if flow_name_exec not in flow_counts:
                    flow_counts[flow_name_exec] = 0
                flow_counts[flow_name_exec] += 1

            if stats["total_executions"] > 0:
                stats["average_duration"] = total_duration / stats["total_executions"]
                stats["total_duration"] = total_duration

            stats["executions_by_flow"] = flow_counts

            return stats

        except Exception as e:
            logger.error(f"Failed to get flow stats: {e}")
            return {"error": str(e)}


# Global metrics collector
metrics_collector = FlowMetrics()


@contextmanager
def monitor_flow(flow_name: str, parameters: Dict[str, Any] = None):
    """Context manager to monitor flow execution."""
    execution_id = metrics_collector.start_flow(flow_name, parameters)

    try:
        yield execution_id
    except Exception as e:
        metrics_collector.record_error(execution_id, str(e))
        metrics_collector.end_flow(execution_id, "failed")
        raise
    else:
        metrics_collector.end_flow(execution_id, "success")


@contextmanager
def monitor_task(execution_id: str, task_name: str):
    """Context manager to monitor task execution."""
    metrics_collector.record_task_start(execution_id, task_name)

    try:
        yield
    except Exception as e:
        metrics_collector.record_task_end(execution_id, task_name, "failed", str(e))
        raise
    else:
        metrics_collector.record_task_end(execution_id, task_name, "success")


def log_flow_event(execution_id: str, event: str, **kwargs) -> None:
    """Log a custom flow event."""
    logger.info(
        f"Flow event: {event}",
        extra={"execution_id": execution_id, "event": event, **kwargs},
    )


def send_alert(message: str, level: str = "warning", **kwargs) -> None:
    """
    Send an alert for critical issues.

    This is a placeholder for integrating with alerting systems
    like Slack, email, PagerDuty, etc.
    """
    alert_data = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": level,
        "message": message,
        **kwargs,
    }

    # Log the alert
    log_level = logging.WARNING if level == "warning" else logging.ERROR
    logger.log(
        log_level,
        f"ALERT [{level.upper()}]: {message}",
        extra={"alert_data": alert_data, "event": "alert"},
    )

    # Here you could integrate with external alerting systems:
    # - Send Slack message
    # - Send email
    # - Create PagerDuty incident
    # - Send SMS, etc.

    print(f"ALERT [{level.upper()}]: {message}")


def health_check() -> Dict[str, Any]:
    """Perform a health check of the monitoring system."""
    health_status = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "status": "healthy",
        "checks": {},
    }

    # Check metrics file
    try:
        if metrics_collector.metrics_file.exists():
            health_status["checks"]["metrics_file"] = "ok"
        else:
            health_status["checks"]["metrics_file"] = "missing"
            health_status["status"] = "degraded"
    except Exception as e:
        health_status["checks"]["metrics_file"] = f"error: {e}"
        health_status["status"] = "unhealthy"

    # Check logs directory
    try:
        logs_dir = Path("logs")
        if logs_dir.exists():
            health_status["checks"]["logs_directory"] = "ok"
        else:
            health_status["checks"]["logs_directory"] = "missing"
            health_status["status"] = "degraded"
    except Exception as e:
        health_status["checks"]["logs_directory"] = f"error: {e}"
        health_status["status"] = "unhealthy"

    # Get recent flow stats
    try:
        stats = metrics_collector.get_flow_stats(hours=1)
        if "error" not in stats:
            health_status["checks"]["flow_stats"] = "ok"
            health_status["recent_stats"] = stats
        else:
            health_status["checks"]["flow_stats"] = f"error: {stats['error']}"
            health_status["status"] = "degraded"
    except Exception as e:
        health_status["checks"]["flow_stats"] = f"error: {e}"
        health_status["status"] = "unhealthy"

    return health_status


def cleanup_old_metrics(days: int = 30) -> int:
    """Clean up old metrics files."""
    try:
        if not metrics_collector.metrics_file.exists():
            return 0

        with open(metrics_collector.metrics_file, "r") as f:
            all_metrics = json.load(f)

        # Filter out old metrics
        cutoff_time = datetime.now(timezone.utc).timestamp() - (days * 24 * 3600)
        filtered_metrics = {}

        for execution_id, metrics in all_metrics.items():
            try:
                start_time = datetime.fromisoformat(metrics["start_time"]).timestamp()
                if start_time >= cutoff_time:
                    filtered_metrics[execution_id] = metrics
            except (KeyError, ValueError):
                # Keep metrics with invalid timestamps
                filtered_metrics[execution_id] = metrics

        # Save filtered metrics
        with open(metrics_collector.metrics_file, "w") as f:
            json.dump(filtered_metrics, f, indent=2, default=str)

        removed_count = len(all_metrics) - len(filtered_metrics)
        logger.info(f"Cleaned up {removed_count} old metrics entries")

        return removed_count

    except Exception as e:
        logger.error(f"Failed to cleanup old metrics: {e}")
        return 0


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Prefect Flow Monitoring")
    parser.add_argument("--stats", action="store_true", help="Show flow statistics")
    parser.add_argument("--health", action="store_true", help="Run health check")
    parser.add_argument(
        "--cleanup",
        type=int,
        metavar="DAYS",
        help="Clean up metrics older than DAYS (default: 30)",
    )
    parser.add_argument("--flow", help="Filter stats by flow name")
    parser.add_argument(
        "--hours", type=int, default=24, help="Hours of history for stats (default: 24)"
    )

    args = parser.parse_args()

    if args.stats:
        print("Flow Statistics")
        stats = metrics_collector.get_flow_stats(args.flow, args.hours)
        print(json.dumps(stats, indent=2, default=str))

    elif args.health:
        print("Health Check")
        health = health_check()
        print(json.dumps(health, indent=2, default=str))

    elif args.cleanup is not None:
        days = args.cleanup if args.cleanup > 0 else 30
        print(f"Cleaning up metrics older than {days} days...")
        removed = cleanup_old_metrics(days)
        print(f"Removed {removed} old metric entries")

    else:
        parser.print_help()
