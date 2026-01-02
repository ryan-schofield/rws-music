#!/usr/bin/env python3
"""Check available node types in n8n instance."""

import sys
sys.path.insert(0, 'flows')
from cli.n8n_client import N8NClient
import json

client = N8NClient()

try:
    # Try to get credential types which sometimes lists node types
    response = client.session.get(f"{client.base_url}api/v1/credential-types", timeout=10)
    print("Credential types available")
except:
    pass

# Check n8n version
try:
    response = client.session.get(f"{client.base_url}api/v1/version", timeout=10)
    if response.status_code == 200:
        version = response.json()
        print(f"\nn8n Version: {version}")
except Exception as e:
    print(f"Could not get version: {e}")

# Try to list available nodes via a different endpoint
try:
    response = client.session.get(f"{client.base_url}api/v1/nodes", timeout=10)
    if response.status_code == 200:
        nodes = response.json()
        print(f"\nAvailable Nodes ({len(nodes)} total):")
        
        # Filter for Python, Code, Execute, Function nodes
        relevant = [n for n in nodes if any(x in n.lower() for x in ['python', 'code', 'execute', 'function', 'bash', 'shell', 'script'])]
        for node in sorted(relevant):
            print(f"  - {node}")
except Exception as e:
    print(f"Could not list nodes: {e}")

# Try to get nodeTypes from the frontend settings
try:
    response = client.session.get(f"{client.base_url}api/v1/n8n/node-types", timeout=10)
    if response.status_code == 200:
        data = response.json()
        print(f"\nNode Types from endpoint:")
        if isinstance(data, list):
            for item in data[:20]:
                if isinstance(item, dict):
                    print(f"  - {item}")
except Exception as e:
    print(f"Could not get node types: {e}")
