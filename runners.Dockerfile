FROM n8nio/runners:latest

USER root

# Create work directory for launcher and app
RUN mkdir -p /tmp/n8n-runners /app && chmod 777 /tmp/n8n-runners /app

# Install Python dependencies that don't require native compilation
# These are the minimal set needed for data transformation and API interaction
# Complex packages like duckdb, polars should be invoked via subprocess calls to the data pipeline container

# API clients and utilities
RUN cd /opt/runners/task-runner-python && uv pip install \
    requests==2.32.0 \
    httpx==0.27.0

# Data enrichment APIs
RUN cd /opt/runners/task-runner-python && uv pip install \
    musicbrainzngs==0.7.1 \
    pycountry>=24.6.1 \
    pycountry-convert>=0.7.2

# Utilities
RUN cd /opt/runners/task-runner-python && uv pip install \
    python-dateutil==2.9.0 \
    python-dotenv==1.0.0 \
    structlog==24.1.0 \
    rich==13.7.0

# Copy custom launcher configuration
RUN mkdir -p /etc/n8n && chown -R runner:runner /etc/n8n
COPY --chown=runner:runner n8n-task-runners.json /etc/n8n/task-runners.json

USER runner
