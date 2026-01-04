FROM n8nio/runners:latest AS launcher

FROM python:3.13-slim-bullseye

# Copy the launcher and its config
COPY --from=launcher /usr/local/bin/task-runner-launcher /usr/local/bin/task-runner-launcher
COPY --from=launcher /etc/n8n-task-runners.json /etc/n8n-task-runners.json

# Modify the task-runners.json to allow external modules and set PYTHONPATH
RUN sed -i 's/"N8N_RUNNERS_STDLIB_ALLOW": ""/"N8N_RUNNERS_STDLIB_ALLOW": "*"/' /etc/n8n-task-runners.json && \
    sed -i 's/"N8N_RUNNERS_EXTERNAL_ALLOW": ""/"N8N_RUNNERS_EXTERNAL_ALLOW": "*"/' /etc/n8n-task-runners.json && \
    sed -i 's|"PYTHONPATH": "/opt/runners/task-runner-python"|"PYTHONPATH": "/opt/runners/task-runner-python:/workspace"|' /etc/n8n-task-runners.json

# Copy the runner source code
COPY --from=launcher /opt/runners /opt/runners

# Install dependencies including tini
RUN apt-get update && apt-get install -y \
    tini \
    gcc \
    g++ \
    python3-dev \
    libffi-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy uv dependency files
COPY pyproject.toml uv.lock /tmp/

# Create the expected .venv structure
RUN rm -rf /opt/runners/task-runner-python/.venv && \
    python -m venv /opt/runners/task-runner-python/.venv && \
    /opt/runners/task-runner-python/.venv/bin/pip install --upgrade pip uv

# Install project dependencies into the venv (single source of truth: pyproject.toml)
RUN cd /tmp && \
    /opt/runners/task-runner-python/.venv/bin/uv pip install --no-cache-dir --python /opt/runners/task-runner-python/.venv/bin/python -e .

# Create runner user and set permissions
RUN useradd -m runner && \
    chown -R runner:runner /opt/runners

USER root

# Set working directory to workspace so relative paths resolve correctly
WORKDIR /workspace

# Set environment variables for the runner
ENV N8N_RUNNER=true
ENV N8N_RUNNER_FEATURES=python

ENTRYPOINT ["/bin/bash", "-c", "mkdir -p /workspace/data/raw/recently_played/detail /workspace/data/cache/mbz /workspace/data/cursor /workspace/data/src /workspace/logs && chmod -R 777 /workspace/data /workspace/logs && chown -R runner:runner /workspace/data /workspace/logs 2>/dev/null || true && ln -sf /workspace /home/runner/workspace 2>/dev/null || true && exec /usr/local/bin/task-runner-launcher python"]
