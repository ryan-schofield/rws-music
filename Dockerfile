FROM python:3.11-slim-bullseye

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    git \
    && apt-get upgrade -y \
    && rm -rf /var/lib/apt/lists/*

# Install uv package manager
RUN pip install uv

# Copy uv dependency files
COPY pyproject.toml uv.lock ./

# Install Python dependencies with uv
RUN uv sync --frozen --no-dev

# Copy application code
COPY . .

# Copy the flows directory specifically to ensure it's accessible
COPY flows/ flows/

# Create necessary directories with proper permissions
RUN mkdir -p /app/data/raw/recently_played/detail && \
    mkdir -p /app/data/cache/mbz && \
    mkdir -p /app/data/cursor && \
    mkdir -p /app/data/src && \
    mkdir -p /app/logs && \
    chmod -R 777 /app/data /app/logs

# Add uv's virtual environment bin directory to PATH
ENV PATH="/app/.venv/bin:$PATH"

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD uv run python -c "import duckdb; print('Health check passed')" || exit 1

# Keep container running indefinitely
CMD ["tail", "-f", "/dev/null"]
