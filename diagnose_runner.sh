#!/bin/bash
# Diagnostic script for n8n task runner issues on Synology NAS

echo "=== System Information ==="
uname -a
cat /etc/os-release 2>/dev/null || echo "OS release info not available"

echo -e "\n=== CPU Architecture ==="
lscpu | grep -E 'Architecture|Model name|Flags' || cat /proc/cpuinfo | head -20

echo -e "\n=== Docker Version ==="
docker --version
docker info | grep -E 'Architecture|OSType|Server Version'

echo -e "\n=== Container Status ==="
docker compose ps

echo -e "\n=== Task Runner Container Logs (last 50 lines) ==="
docker logs --tail 50 music-tracker-n8n-runners

echo -e "\n=== n8n Container Logs (last 30 lines) ==="
docker logs --tail 30 music-tracker-n8n

echo -e "\n=== Task Runner Python Test ==="
docker exec music-tracker-n8n-runners python --version
docker exec music-tracker-n8n-runners python -c "import sys; print(f'Python: {sys.version}'); print(f'Executable: {sys.executable}')"

echo -e "\n=== Test Package Imports ==="
docker exec music-tracker-n8n-runners python -c "
try:
    import polars
    print(f'✓ polars {polars.__version__}')
except Exception as e:
    print(f'✗ polars: {e}')

try:
    import duckdb
    print(f'✓ duckdb {duckdb.__version__}')
except Exception as e:
    print(f'✗ duckdb: {e}')

try:
    import flows
    print('✓ flows module')
except Exception as e:
    print(f'✗ flows: {e}')
"

echo -e "\n=== Test Simple Python Execution ==="
docker exec music-tracker-n8n-runners python -c "print('Hello from Python')"

echo -e "\n=== Check Task Runner Process ==="
docker exec music-tracker-n8n-runners ps aux

echo -e "\n=== Network Connectivity ==="
docker exec music-tracker-n8n-runners ping -c 2 n8n || echo "Cannot reach n8n container"

echo -e "\n=== Memory Usage ==="
docker stats --no-stream music-tracker-n8n-runners

echo -e "\n=== Environment Variables in Task Runner ==="
docker exec music-tracker-n8n-runners env | grep -E 'N8N|PYTHON|PATH' | sort
