#!/usr/bin/env python3
"""
Batch processing utilities for n8n workflows.

Provides utilities for splitting large datasets into manageable batches
with state tracking and resumption capabilities.
"""

import json
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class BatchProcessor:
    """
    Manages batch processing state and coordination for n8n workflows.
    """

    def __init__(self, state_dir: str = "data/cursor"):
        """
        Initialize batch processor.

        Args:
            state_dir: Directory to store batch processing state
        """
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)

    def create_batch_plan(
        self,
        total_items: int,
        batch_size: int,
        workflow_id: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Create a batch processing plan.

        Args:
            total_items: Total number of items to process
            batch_size: Number of items per batch
            workflow_id: Unique identifier for this workflow run
            metadata: Additional metadata to store

        Returns:
            Batch plan with configuration and state tracking
        """
        num_batches = (total_items + batch_size - 1) // batch_size

        plan = {
            "workflow_id": workflow_id,
            "created_at": datetime.utcnow().isoformat(),
            "total_items": total_items,
            "batch_size": batch_size,
            "num_batches": num_batches,
            "batches_completed": 0,
            "status": "pending",
            "metadata": metadata or {},
            "batches": [
                {
                    "batch_index": i,
                    "offset": i * batch_size,
                    "size": min(batch_size, total_items - i * batch_size),
                    "status": "pending",
                    "started_at": None,
                    "completed_at": None,
                    "error": None,
                }
                for i in range(num_batches)
            ],
        }

        # Save plan to disk
        self._save_plan(workflow_id, plan)

        logger.info(
            f"Created batch plan '{workflow_id}' with {num_batches} batches "
            f"({total_items} items, {batch_size} per batch)"
        )

        return plan

    def get_plan(self, workflow_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve an existing batch plan.

        Args:
            workflow_id: Workflow identifier

        Returns:
            Batch plan or None if not found
        """
        plan_file = self.state_dir / f"{workflow_id}.json"

        if not plan_file.exists():
            return None

        try:
            with open(plan_file, "r") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading batch plan {workflow_id}: {e}")
            return None

    def update_batch_status(
        self,
        workflow_id: str,
        batch_index: int,
        status: str,
        error: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Update the status of a specific batch.

        Args:
            workflow_id: Workflow identifier
            batch_index: Index of the batch to update
            status: New status ('pending', 'processing', 'completed', 'failed')
            error: Error message if status is 'failed'

        Returns:
            Updated batch plan
        """
        plan = self.get_plan(workflow_id)

        if not plan:
            raise ValueError(f"Batch plan '{workflow_id}' not found")

        if batch_index >= len(plan["batches"]):
            raise ValueError(f"Invalid batch_index {batch_index}")

        batch = plan["batches"][batch_index]
        old_status = batch["status"]
        batch["status"] = status

        if status == "processing" and batch["started_at"] is None:
            batch["started_at"] = datetime.utcnow().isoformat()

        if status == "completed":
            batch["completed_at"] = datetime.utcnow().isoformat()
            if old_status != "completed":
                plan["batches_completed"] += 1

        if status == "failed":
            batch["error"] = error
            batch["completed_at"] = datetime.utcnow().isoformat()

        # Update overall plan status
        all_completed = all(b["status"] == "completed" for b in plan["batches"])
        any_failed = any(b["status"] == "failed" for b in plan["batches"])

        if all_completed:
            plan["status"] = "completed"
        elif any_failed:
            plan["status"] = "partial_failure"
        else:
            plan["status"] = "in_progress"

        self._save_plan(workflow_id, plan)

        logger.info(
            f"Updated batch {batch_index} in workflow '{workflow_id}' to status '{status}'"
        )

        return plan

    def get_next_batch(self, workflow_id: str) -> Optional[Dict[str, Any]]:
        """
        Get the next pending batch to process.

        Args:
            workflow_id: Workflow identifier

        Returns:
            Next batch to process or None if all are completed/processing
        """
        plan = self.get_plan(workflow_id)

        if not plan:
            return None

        for batch in plan["batches"]:
            if batch["status"] == "pending":
                return batch

        return None

    def get_failed_batches(self, workflow_id: str) -> List[Dict[str, Any]]:
        """
        Get all failed batches for retry.

        Args:
            workflow_id: Workflow identifier

        Returns:
            List of failed batches
        """
        plan = self.get_plan(workflow_id)

        if not plan:
            return []

        return [b for b in plan["batches"] if b["status"] == "failed"]

    def get_plan_summary(self, workflow_id: str) -> Dict[str, Any]:
        """
        Get a summary of the batch plan status.

        Args:
            workflow_id: Workflow identifier

        Returns:
            Summary with counts and status
        """
        plan = self.get_plan(workflow_id)

        if not plan:
            return {"exists": False}

        status_counts = {}
        for batch in plan["batches"]:
            status = batch["status"]
            status_counts[status] = status_counts.get(status, 0) + 1

        return {
            "exists": True,
            "workflow_id": workflow_id,
            "status": plan["status"],
            "total_items": plan["total_items"],
            "batch_size": plan["batch_size"],
            "num_batches": plan["num_batches"],
            "batches_completed": plan["batches_completed"],
            "status_counts": status_counts,
            "created_at": plan["created_at"],
        }

    def _save_plan(self, workflow_id: str, plan: Dict[str, Any]):
        """Save batch plan to disk."""
        plan_file = self.state_dir / f"{workflow_id}.json"

        with open(plan_file, "w") as f:
            json.dump(plan, f, indent=2)

    def cleanup_old_plans(self, days: int = 7):
        """
        Remove batch plans older than specified days.

        Args:
            days: Number of days to keep plans
        """
        from datetime import timedelta

        cutoff = datetime.utcnow() - timedelta(days=days)

        for plan_file in self.state_dir.glob("*.json"):
            try:
                with open(plan_file, "r") as f:
                    plan = json.load(f)

                created_at = datetime.fromisoformat(plan["created_at"])

                if created_at < cutoff:
                    plan_file.unlink()
                    logger.info(f"Removed old batch plan: {plan_file.name}")

            except Exception as e:
                logger.warning(f"Error processing plan file {plan_file}: {e}")


def split_into_batches(items: List[Any], batch_size: int) -> List[List[Any]]:
    """
    Split a list into batches of specified size.

    Args:
        items: List of items to split
        batch_size: Maximum size of each batch

    Returns:
        List of batches
    """
    return [items[i : i + batch_size] for i in range(0, len(items), batch_size)]


def create_n8n_batch_items(
    items: List[Any], batch_size: int, item_key: str = "item"
) -> List[Dict[str, Any]]:
    """
    Create n8n-compatible batch items with metadata.

    Args:
        items: List of items to batch
        batch_size: Maximum size of each batch
        item_key: Key name for items in output

    Returns:
        List of dictionaries with batch_index and items
    """
    batches = split_into_batches(items, batch_size)

    return [
        {
            "batch_index": i,
            "batch_size": len(batch),
            "total_batches": len(batches),
            item_key: batch,
        }
        for i, batch in enumerate(batches)
    ]
