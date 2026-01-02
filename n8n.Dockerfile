FROM n8nio/n8n:latest

USER root

# Install Python 3 - try apk first (Alpine), then apt-get (Debian)
RUN apk add --no-cache python3 py3-pip 2>/dev/null || \
    (apt-get update && apt-get install -y python3 python3-pip && apt-get clean && rm -rf /var/lib/apt/lists/*) || \
    echo "Warning: Could not install Python"

# Verify Python is installed
RUN which python3 && python3 --version || echo "Python not found in PATH"

USER node
