#!/usr/bin/env python3
"""
n8n API client for workflow management.

Provides utilities for interacting with n8n REST API to create, update, retrieve,
and export workflows programmatically.
"""

import os
import json
import logging
from typing import Dict, List, Any, Optional
from pathlib import Path
import requests
from urllib.parse import urljoin

logger = logging.getLogger(__name__)


class N8NClient:
    """Client for interacting with n8n API."""

    def __init__(
        self,
        base_url: str = None,
        api_key: str = None,
    ):
        """
        Initialize n8n API client.
        
        Args:
            base_url: n8n instance URL (default: constructed from N8N_HOST and N8N_PORT env vars)
            api_key: n8n API key (optional, default: from N8N_API_KEY env var)
        """
        # If base_url not provided, construct from environment variables
        if not base_url:
            n8n_host = os.getenv("N8N_HOST", "localhost")
            n8n_port = os.getenv("N8N_PORT", "5678")
            n8n_protocol = os.getenv("N8N_PROTOCOL", "http")
            # Try N8N_BASE_URL first for backward compatibility
            base_url = os.getenv("N8N_BASE_URL", f"{n8n_protocol}://{n8n_host}:{n8n_port}")
        
        self.base_url = base_url
        self.api_key = api_key or os.getenv("N8N_API_KEY", "")
        
        # Ensure base_url ends with /
        if not self.base_url.endswith("/"):
            self.base_url += "/"
        
        self.session = requests.Session()
        if self.api_key:
            self.session.headers.update({
                "X-N8N-API-KEY": self.api_key,
            })

    def is_accessible(self) -> bool:
        """
        Check if n8n instance is accessible.
        
        Returns:
            True if accessible, False otherwise
        """
        try:
            response = self.session.get(
                urljoin(self.base_url, "api/v1/workflows"),
                timeout=5,
            )
            return response.status_code in (200, 401)  # 401 if auth required but server up
        except Exception as e:
            logger.error(f"Failed to connect to n8n at {self.base_url}: {str(e)}")
            return False

    def list_workflows(self) -> List[Dict[str, Any]]:
        """
        List all workflows.
        
        Returns:
            List of workflow metadata
        """
        try:
            response = self.session.get(
                urljoin(self.base_url, "api/v1/workflows"),
                timeout=10,
            )
            response.raise_for_status()
            data = response.json()
            return data.get("data", []) if isinstance(data, dict) else data
        except Exception as e:
            logger.error(f"Failed to list workflows: {str(e)}")
            return []

    def get_workflow(self, workflow_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a specific workflow by ID.
        
        Args:
            workflow_id: Workflow ID
            
        Returns:
            Workflow definition or None if not found
        """
        try:
            response = self.session.get(
                urljoin(self.base_url, f"api/v1/workflows/{workflow_id}"),
                timeout=10,
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to get workflow {workflow_id}: {str(e)}")
            return None

    def find_workflow_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Find a workflow by name.
        
        Args:
            name: Workflow name
            
        Returns:
            Workflow metadata or None if not found
        """
        workflows = self.list_workflows()
        for workflow in workflows:
            if workflow.get("name") == name:
                return workflow
        return None

    def create_workflow(self, definition: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Create a new workflow.
        
        Args:
            definition: Workflow definition JSON
            
        Returns:
            Created workflow metadata or None on failure
        """
        try:
            # Log the definition for debugging
            logger.debug(f"Creating workflow with definition: {json.dumps(definition, indent=2)}")
            logger.info(f"Sending POST /workflows with keys: {list(definition.keys())}")
            
            response = self.session.post(
                urljoin(self.base_url, "api/v1/workflows"),
                json=definition,
                timeout=30,
            )
            response.raise_for_status()
            result = response.json()
            logger.info(f"Created workflow: {result.get('id')} ({definition.get('name')})")
            return result
        except requests.exceptions.HTTPError as e:
            # Try to get detailed error from response
            try:
                error_detail = e.response.json()
                logger.error(f"Failed to create workflow: {e.response.status_code} - {error_detail}")
                # Also log the request body that caused the error
                logger.error(f"Request body keys: {list(definition.keys())}")
            except:
                logger.error(f"Failed to create workflow: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Failed to create workflow: {str(e)}")
            return None

    def update_workflow(self, workflow_id: str, definition: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Update an existing workflow.
        
        Args:
            workflow_id: Workflow ID
            definition: Updated workflow definition JSON
            
        Returns:
            Updated workflow metadata or None on failure
        """
        try:
            response = self.session.patch(
                urljoin(self.base_url, f"api/v1/workflows/{workflow_id}"),
                json=definition,
                timeout=30,
            )
            response.raise_for_status()
            result = response.json()
            logger.info(f"Updated workflow: {workflow_id}")
            return result
        except Exception as e:
            logger.error(f"Failed to update workflow {workflow_id}: {str(e)}")
            return None

    def delete_workflow(self, workflow_id: str) -> bool:
        """
        Delete a workflow.
        
        Args:
            workflow_id: Workflow ID
            
        Returns:
            True if successful, False otherwise
        """
        try:
            response = self.session.delete(
                urljoin(self.base_url, f"api/v1/workflows/{workflow_id}"),
                timeout=10,
            )
            response.raise_for_status()
            logger.info(f"Deleted workflow: {workflow_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete workflow {workflow_id}: {str(e)}")
            return False

    def activate_workflow(self, workflow_id: str) -> bool:
        """
        Activate a workflow.
        
        Args:
            workflow_id: Workflow ID
            
        Returns:
            True if successful, False otherwise
        """
        try:
            response = self.session.patch(
                urljoin(self.base_url, f"api/v1/workflows/{workflow_id}"),
                json={"active": True},
                timeout=10,
            )
            response.raise_for_status()
            logger.info(f"Activated workflow: {workflow_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to activate workflow {workflow_id}: {str(e)}")
            return False

    def deactivate_workflow(self, workflow_id: str) -> bool:
        """
        Deactivate a workflow.
        
        Args:
            workflow_id: Workflow ID
            
        Returns:
            True if successful, False otherwise
        """
        try:
            response = self.session.patch(
                urljoin(self.base_url, f"api/v1/workflows/{workflow_id}"),
                json={"active": False},
                timeout=10,
            )
            response.raise_for_status()
            logger.info(f"Deactivated workflow: {workflow_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to deactivate workflow {workflow_id}: {str(e)}")
            return False

    def export_workflow(self, workflow_id: str, filepath: Path) -> bool:
        """
        Export a workflow to JSON file.
        
        Args:
            workflow_id: Workflow ID
            filepath: Destination file path
            
        Returns:
            True if successful, False otherwise
        """
        try:
            workflow = self.get_workflow(workflow_id)
            if not workflow:
                return False
            
            filepath.parent.mkdir(parents=True, exist_ok=True)
            with open(filepath, "w") as f:
                json.dump(workflow, f, indent=2)
            
            logger.info(f"Exported workflow to {filepath}")
            return True
        except Exception as e:
            logger.error(f"Failed to export workflow {workflow_id}: {str(e)}")
            return False

    def import_workflow(self, filepath: Path) -> Optional[Dict[str, Any]]:
        """
        Import a workflow from JSON file.
        
        Args:
            filepath: Source file path
            
        Returns:
            Created workflow metadata or None on failure
        """
        try:
            with open(filepath, "r") as f:
                definition = json.load(f)
            
            return self.create_workflow(definition)
        except Exception as e:
            logger.error(f"Failed to import workflow from {filepath}: {str(e)}")
            return None
