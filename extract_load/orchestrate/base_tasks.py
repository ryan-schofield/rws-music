#!/usr/bin/env python3
"""
Base task classes for Prefect flows.

These base classes eliminate code duplication and provide consistent
patterns for error handling, logging, and result processing.
"""

import sys
import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, Type, TypeVar, Generic
from dataclasses import asdict

from prefect import task, get_run_logger
from prefect.states import Failed

# Add project root to path for imports (more robust approach)
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from extract_load.orchestrate.flow_config import FlowConfig
from extract_load.enrich.utils.data_writer import ParquetDataWriter

# Type variables for generic base classes
T = TypeVar("T")


class TaskResult:
    """Standardized task result format."""

    def __init__(
        self,
        status: str,
        message: str = "",
        data: Optional[Dict[str, Any]] = None,
        metrics: Optional[Dict[str, Any]] = None,
        errors: Optional[list] = None,
    ):
        self.status = status
        self.message = message
        self.data = data or {}
        self.metrics = metrics or {}
        self.errors = errors or []
        self.timestamp = datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "status": self.status,
            "message": self.message,
            "data": self.data,
            "metrics": self.metrics,
            "errors": self.errors,
            "timestamp": self.timestamp,
        }

    def is_success(self) -> bool:
        """Check if task completed successfully."""
        return self.status in ["success", "no_updates", "partial_success"]

    def is_failure(self) -> bool:
        """Check if task failed."""
        return self.status in ["error", "failed"]


class BaseTask(ABC):
    """Abstract base class for all task implementations."""

    def __init__(
        self, config: FlowConfig, data_writer: Optional[ParquetDataWriter] = None
    ):
        self.config = config
        self.data_writer = data_writer or ParquetDataWriter(str(config.data_base_path))
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def execute(self, **kwargs) -> TaskResult:
        """Execute the task logic. Must be implemented by subclasses."""
        pass

    def _handle_error(self, error: Exception, context: str = "") -> TaskResult:
        """Standardized error handling."""
        error_msg = f"{context}: {str(error)}" if context else str(error)
        self.logger.error(error_msg, exc_info=True)

        return TaskResult(
            status="error",
            message=error_msg,
            errors=[{"type": type(error).__name__, "message": str(error)}],
        )

    def _validate_prerequisites(self) -> TaskResult:
        """Validate that required data/conditions exist before execution."""
        # Default implementation - override in subclasses as needed
        return TaskResult(status="success", message="Prerequisites validated")


class BaseProcessorTask(BaseTask):
    """Base class for tasks that use processor classes directly."""

    def __init__(self, config: FlowConfig, processor_class: Type[T]):
        super().__init__(config)
        self.processor_class = processor_class
        self._processor_instance = None

    @property
    def processor(self) -> T:
        """Lazy initialization of processor instance."""
        if self._processor_instance is None:
            self._processor_instance = self.processor_class(self.data_writer)
        return self._processor_instance

    def execute(self, **kwargs) -> TaskResult:
        """Execute using processor class."""
        try:
            self.logger.info("Starting processor task execution")

            # Validate prerequisites first
            self.logger.info("Validating prerequisites")
            prereq_result = self._validate_prerequisites()
            self.logger.info(f"Prerequisite validation result: {prereq_result.status}")

            if prereq_result.is_failure():
                self.logger.error(
                    f"Prerequisite validation failed: {prereq_result.message}"
                )
                return prereq_result

            # Execute the processor method
            self.logger.info("Executing processor method")
            result = self._execute_processor(**kwargs)
            self.logger.info(
                f"Processor method result: {result.get('status', 'unknown')}"
            )

            # Convert processor result to TaskResult
            task_result = self._convert_processor_result(result)
            self.logger.info(f"Task result: {task_result.status}")
            return task_result

        except Exception as e:
            self.logger.error(
                f"Processor execution failed with exception: {e}", exc_info=True
            )
            return self._handle_error(e, f"Processor execution failed")

    @abstractmethod
    def _execute_processor(self, **kwargs) -> Dict[str, Any]:
        """Execute the specific processor method. Must be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement _execute_processor")

    def _convert_processor_result(self, result: Dict[str, Any]) -> TaskResult:
        """Convert processor result format to TaskResult."""
        status = result.get("status", "unknown")
        message = result.get("message", "")

        # Extract metrics if available
        metrics = {}
        for key, value in result.items():
            if key.endswith(
                ("_processed", "_written", "_updated", "_found", "_failed")
            ):
                metrics[key] = value

        return TaskResult(status=status, message=message, data=result, metrics=metrics)


class BaseEnrichmentTask(BaseProcessorTask):
    """Base class for enrichment tasks with common patterns."""

    def _validate_prerequisites(self) -> TaskResult:
        """Validate that source data exists for enrichment."""
        if not self.data_writer.table_exists("tracks_played"):
            return TaskResult(
                status="error",
                message="tracks_played table not found - cannot perform enrichment",
            )

        tracks_info = self.data_writer.get_table_info("tracks_played")
        if tracks_info.get("record_count", 0) == 0:
            return TaskResult(
                status="error",
                message="tracks_played table is empty - cannot perform enrichment",
            )

        return TaskResult(
            status="success",
            message=f"Prerequisites validated - {tracks_info.get('record_count', 0)} tracks available",
        )


# Prefect task decorators for each base class
def create_processor_task(
    task_class: Type[BaseTask],
    processor_class: Type[T],
    task_name: str,
    description: str = "",
    retries: int = 2,
    retry_delay_seconds: int = 30,
    timeout_seconds: int = 300,
):
    """Factory function to create Prefect tasks from processor classes."""

    @task(
        name=task_name,
        description=description,
        retries=retries,
        retry_delay_seconds=retry_delay_seconds,
        timeout_seconds=timeout_seconds,
    )
    def processor_task_wrapper(config: FlowConfig, **kwargs) -> Dict[str, Any]:
        """Wrapper function that creates and executes the processor task."""
        logger = get_run_logger()

        try:
            logger.info(f"Starting {task_name} task wrapper")

            # Create task instance
            logger.info(f"Creating {task_class.__name__} instance")
            task_instance = task_class(config, processor_class)
            logger.info("Task instance created successfully")

            # Execute task
            logger.info("Executing task")
            result = task_instance.execute(**kwargs)
            logger.info(f"Task execution completed with status: {result.status}")

            # Log result
            if result.is_success():
                logger.info(f"{task_name} completed: {result.message}")
                return result.to_dict()
            else:
                logger.error(f"{task_name} failed: {result.message}")
                # Return Failed state to properly fail the Prefect task
                return Failed(message=f"{task_name} failed: {result.message}")

        except Exception as e:
            error_msg = f"{task_name} failed with exception: {str(e)}"
            logger.error(error_msg, exc_info=True)

            raise RuntimeError(error_msg)

    return processor_task_wrapper


def create_enrichment_task(
    task_class: Type[BaseEnrichmentTask],
    processor_class: Type[T],
    task_name: str,
    description: str = "",
    retries: int = 2,
    retry_delay_seconds: int = 30,
    timeout_seconds: int = 900,
):
    """Factory function specifically for enrichment tasks."""
    return create_processor_task(
        task_class=task_class,
        processor_class=processor_class,
        task_name=task_name,
        description=description,
        retries=retries,
        retry_delay_seconds=retry_delay_seconds,
        timeout_seconds=timeout_seconds,
    )


class TaskMetrics:
    """Helper class for collecting and reporting task metrics."""

    def __init__(self):
        self.start_time = datetime.now(timezone.utc)
        self.metrics = {}

    def record(self, key: str, value: Any) -> None:
        """Record a metric value."""
        self.metrics[key] = value

    def increment(self, key: str, value: int = 1) -> None:
        """Increment a counter metric."""
        self.metrics[key] = self.metrics.get(key, 0) + value

    def finalize(self) -> Dict[str, Any]:
        """Finalize metrics with timing information."""
        end_time = datetime.now(timezone.utc)
        duration = (end_time - self.start_time).total_seconds()

        return {
            **self.metrics,
            "duration_seconds": duration,
            "start_time": self.start_time.isoformat(),
            "end_time": end_time.isoformat(),
        }


class ValidationHelper:
    """Helper class for common validation patterns."""

    @staticmethod
    def validate_table_exists(
        data_writer: ParquetDataWriter, table_name: str
    ) -> TaskResult:
        """Validate that a table exists and has data."""
        if not data_writer.table_exists(table_name):
            return TaskResult(
                status="error", message=f"Required table '{table_name}' not found"
            )

        info = data_writer.get_table_info(table_name)
        record_count = info.get("record_count", 0)

        if record_count == 0:
            return TaskResult(
                status="error", message=f"Table '{table_name}' exists but is empty"
            )

        return TaskResult(
            status="success",
            message=f"Table '{table_name}' validated with {record_count} records",
        )

    @staticmethod
    def validate_config_limits(config: FlowConfig, **limits) -> TaskResult:
        """Validate that configuration limits are reasonable."""
        errors = []

        for limit_name, limit_value in limits.items():
            if limit_value is not None and limit_value <= 0:
                errors.append(f"Invalid {limit_name}: {limit_value} (must be positive)")

        if errors:
            return TaskResult(
                status="error", message="Configuration validation failed", errors=errors
            )

        return TaskResult(status="success", message="Configuration limits validated")
