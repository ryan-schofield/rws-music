# syntax=docker/dockerfile:1.6
# Full Debian-based task runner for n8n with polars-lts-cpu support
# This approach uses Debian throughout to get pre-built wheels for all packages

# Stage 1: Extract task-runner-launcher from official n8n runners image
FROM n8nio/runners:2.2.3 as launcher-source

# Stage 2: Build our custom Debian runner
FROM python:3.13-slim as base

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    tini \
    && rm -rf /var/lib/apt/lists/*

# Install uv for fast package installation
RUN pip install --no-cache-dir uv

# Copy dependency definition files
COPY pyproject.toml uv.lock /tmp/

# Install all Python packages from pyproject.toml using pre-built wheels (fast on Debian)
RUN uv pip install --system --no-cache-dir -r /tmp/pyproject.toml

# Ensure dbt is accessible (it should be in /usr/local/bin after uv pip install --system)
RUN which dbt || python -m dbt --version || echo "Warning: dbt not found in expected locations"

# Create runner user
RUN groupadd -g 1000 runner && \
    useradd -u 1000 -g runner -m -s /bin/bash runner

# Set up workspace
WORKDIR /home/runner/workspace
RUN chown -R runner:runner /home/runner

# Copy task-runner-python from official n8n runners image (contains the n8n Python task runner)
COPY --from=launcher-source /opt/runners/task-runner-python /opt/runners/task-runner-python

# Copy task-runner-launcher from official n8n runners image
COPY --from=launcher-source /usr/local/bin/task-runner-launcher /usr/local/bin/task-runner-launcher

# Copy task runner configuration
COPY n8n-task-runners.json /etc/n8n-task-runners.json

# Run as root to avoid permission issues with Synology volumes
# USER runner
WORKDIR /home/runner/workspace

EXPOSE 5682/tcp

ENTRYPOINT ["tini", "--", "/usr/local/bin/task-runner-launcher"]
CMD ["python"]