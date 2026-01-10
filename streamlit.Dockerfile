FROM python:3.11-slim-bullseye

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv package manager
RUN pip install uv

# Copy dependency files
COPY pyproject.toml uv.lock ./

# Install Python dependencies
RUN uv sync --frozen --no-dev

# Copy application code
COPY streamlit/ streamlit/

# Copy config for DuckDB access
COPY .env ./

# Add uv's virtual environment bin directory to PATH
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1

# Streamlit configuration
ENV STREAMLIT_SERVER_PORT=8501
ENV STREAMLIT_SERVER_HEADLESS=true
ENV STREAMLIT_SERVER_RUNONSAVE=false
ENV STREAMLIT_CLIENT_TOOLBAR_MODE=viewer

EXPOSE 8501

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8501/_stcore/health || exit 1

# Streamlit multi-page apps require setting the PYTHONPATH to find the streamlit module
CMD ["streamlit", "run", "streamlit/app.py", "--logger.level=info", "--client.showErrorDetails=true"]
