#!/usr/bin/env python3
"""
n8n workflow definitions.

Provides builders and definitions for n8n workflows that orchestrate the data pipeline.
"""

from typing import Dict, Any, List, Optional
from datetime import datetime
import json


class N8NNode:
    """Represents an n8n workflow node."""

    def __init__(
        self,
        name: str,
        type: str,
        position: tuple = None,
        parameters: Dict[str, Any] = None,
        credentials: Dict[str, Any] = None,
        typeVersion: int = None,
    ):
        """
        Initialize workflow node.
        
        Args:
            name: Node name
            type: Node type (e.g., 'n8n-nodes-base.webhook', 'n8n-nodes-base.httpRequest')
            position: (x, y) coordinates
            parameters: Node parameters
            credentials: Credential references
            typeVersion: Node type version (e.g., 1, 2)
        """
        self.name = name
        self.type = type
        self.position = position or [0, 0]
        self.parameters = parameters or {}
        self.credentials = credentials or {}
        self.typeVersion = typeVersion

    def to_dict(self) -> Dict[str, Any]:
        """Convert to n8n node JSON."""
        node_dict = {
            "name": self.name,
            "type": self.type,
            "position": self.position,
            "parameters": self.parameters,
            "credentials": self.credentials,
        }
        if self.typeVersion is not None:
            node_dict["typeVersion"] = self.typeVersion
        return node_dict


class N8NConnection:
    """Represents a connection between nodes."""

    def __init__(self, source_node: str, target_node: str):
        """
        Initialize connection.
        
        Args:
            source_node: Source node name
            target_node: Target node name
        """
        self.source_node = source_node
        self.target_node = target_node

    def to_dict(self) -> Dict[str, Any]:
        """Convert to n8n connection JSON."""
        return {
            self.source_node: [{"node": self.target_node, "type": "main", "index": 0}],
        }


class N8NWorkflow:
    """Builder for n8n workflows."""

    def __init__(self, name: str, description: str = "", active: bool = False):
        """
        Initialize workflow builder.
        
        Args:
            name: Workflow name
            description: Workflow description
            active: Whether workflow is active
        """
        self.name = name
        self.description = description
        self.active = active
        self.nodes: List[N8NNode] = []
        self.connections: Dict[str, List[Dict[str, Any]]] = {}
        self.settings: Dict[str, Any] = {}

    def add_node(self, node: N8NNode) -> "N8NWorkflow":
        """Add a node to the workflow."""
        self.nodes.append(node)
        return self

    def connect(self, source: str, target: str) -> "N8NWorkflow":
        """Connect two nodes."""
        if source not in self.connections:
            self.connections[source] = []
        self.connections[source].append({
            "node": target,
            "type": "main",
            "index": 0,
        })
        return self

    def set_settings(self, settings: Dict[str, Any]) -> "N8NWorkflow":
        """Set workflow settings."""
        self.settings = settings
        return self

    def to_dict(self) -> Dict[str, Any]:
        """Convert to n8n workflow JSON for API submission."""
        # Format connections properly for n8n API
        # Each node source needs to specify its outputs
        connections = {}
        for source_node, targets in self.connections.items():
            connections[source_node] = {
                "main": [
                    [{"node": target["node"], "type": "main", "index": 0} for target in targets]
                ]
            }
        
        # Build payload for API submission
        # Note: 'active' is read-only and must NOT be included in POST/PATCH requests
        payload = {
            "name": self.name,
            "nodes": [node.to_dict() for node in self.nodes],
            "connections": connections,
            "settings": {},
        }
        
        return payload


def create_cli_execution_node(
    node_name: str,
    cli_script: str,
    cli_args: Dict[str, str] = None,
    position: tuple = None,
) -> N8NNode:
    """
    Create a node that executes Python code via n8n's Code node with Python runtime.
    
    Args:
        node_name: Node name
        cli_script: Path to CLI script (e.g., "flows/cli/ingest_spotify.py")
        cli_args: Dictionary of CLI arguments to pass
        position: (x, y) position
        
    Returns:
        Configured N8NNode with n8n-nodes-base.code type v2 and pythonNative runtime
    """
    # Build the Python code that will be executed
    args_dict = cli_args or {}
    
    python_code = f"""import subprocess
import os

script_path = "{cli_script}"
args = {repr(args_dict)}

os.chdir("/app")

# Build command
cmd = ["uv", "run", "python", script_path]
for key, value in args.items():
    if value is True:
        cmd.append(f"--{{key}}")
    elif value is not False:
        cmd.append(f"--{{key}}")
        cmd.append(str(value))

# Execute script
result = subprocess.run(cmd, capture_output=True, text=True)
print(result.stdout)
if result.stderr:
    print(f"Error: {{result.stderr}}")
if result.returncode != 0:
    raise Exception(f"Script failed with return code {{result.returncode}}")
"""

    return N8NNode(
        name=node_name,
        type="n8n-nodes-base.code",
        position=position,
        parameters={
            "language": "pythonNative",
            "pythonCode": python_code,
        },
        typeVersion=2,
    )


def create_http_webhook_node(
    node_name: str,
    path: str,
    position: tuple = None,
) -> N8NNode:
    """
    Create an HTTP webhook trigger node.
    
    Args:
        node_name: Node name
        path: Webhook path
        position: (x, y) position
        
    Returns:
        Configured N8NNode
    """
    return N8NNode(
        name=node_name,
        type="n8n-nodes-base.webhook",
        position=position,
        parameters={
            "path": path,
            "httpMethod": "POST",
            "responseMode": "lastNode",
            "options": {},
        },
    )


def create_cron_trigger_node(
    node_name: str,
    expression: str,
    position: tuple = None,
) -> N8NNode:
    """
    Create a cron trigger node for scheduled execution.
    
    Args:
        node_name: Node name
        expression: Cron expression (e.g., "0 2 * * *" for daily at 2 AM)
        position: (x, y) position
        
    Returns:
        Configured N8NNode
    """
    return N8NNode(
        name=node_name,
        type="n8n-nodes-base.cron",
        position=position,
        parameters={
            "mode": "cron",
            "expression": expression,
        },
    )


def create_condition_node(
    node_name: str,
    conditions: List[Dict[str, Any]],
    position: tuple = None,
) -> N8NNode:
    """
    Create a conditional logic node.
    
    Args:
        node_name: Node name
        conditions: List of condition definitions
        position: (x, y) position
        
    Returns:
        Configured N8NNode
    """
    return N8NNode(
        name=node_name,
        type="n8n-nodes-base.if",
        position=position,
        parameters={
            "conditions": conditions,
        },
    )


def create_webhook_response_node(
    node_name: str,
    status_code: int = 200,
    position: tuple = None,
) -> N8NNode:
    """
    Create a webhook response node.
    
    Args:
        node_name: Node name
        status_code: HTTP status code
        position: (x, y) position
        
    Returns:
        Configured N8NNode
    """
    return N8NNode(
        name=node_name,
        type="n8n-nodes-base.respondToWebhook",
        position=position,
        parameters={
            "responseCode": status_code,
            "options": {},
        },
    )


def create_wait_node(
    node_name: str,
    wait_type: str = "seconds",
    amount: int = 60,
    position: tuple = None,
) -> N8NNode:
    """
    Create a wait node to delay execution.
    
    Args:
        node_name: Node name
        wait_type: Type of wait ('seconds', 'minutes', 'hours')
        amount: Amount to wait
        position: (x, y) position
        
    Returns:
        Configured N8NNode
    """
    return N8NNode(
        name=node_name,
        type="n8n-nodes-base.wait",
        position=position,
        parameters={
            "mode": "timeInterval",
            "timeInterval": amount,
            "timeUnit": wait_type,
        },
    )
