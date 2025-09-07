#!/bin/bash
# Simplified setup for internal-only Music Tracker deployment
set -e

# Update system
apt-get update && apt-get upgrade -y

# Install Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sh get-docker.sh
usermod -aG docker ubuntu

# Install Docker Compose
curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
chmod +x /usr/local/bin/docker-compose

# Create application directory
mkdir -p /opt/music-tracker
cd /opt/music-tracker

# Create directories for persistent data
mkdir -p data logs scripts

# Create internal-only docker-compose configuration
cat > docker-compose.internal.yml << 'EOF'
services:
  # Main data processing and dbt service
  data-pipeline:
    build: 
      context: .
      dockerfile: Dockerfile
    container_name: music-tracker-pipeline
    volumes:
      - ./data:/app/data
      - ./dbt:/app/dbt
      - ./flows:/app/flows
      - ./scripts:/app/scripts
      - ./logs:/app/logs
    environment:
      - DUCKDB_PATH=/app/data/music_tracker.duckdb
      - LOG_LEVEL=INFO
      - SPOTIFY_CLIENT_ID=${SPOTIFY_CLIENT_ID}
      - SPOTIFY_CLIENT_SECRET=${SPOTIFY_CLIENT_SECRET}
      - METABASE_DB_PASSWORD=${METABASE_DB_PASSWORD}
      - PREFECT_DB_PASSWORD=${PREFECT_DB_PASSWORD}
    networks:
      - music-tracker
    depends_on:
      - metabase
    restart: unless-stopped

  # Metabase for reporting (internal access)
  metabase:
    build:
      context: .
      dockerfile: metabase.Dockerfile
    container_name: music-tracker-metabase
    ports:
      - "3000:3000"  # Direct access for internal use
    volumes:
      - metabase-data:/home
      - ./data:/data
    environment:
      - MB_DB_TYPE=postgres
      - MB_DB_DBNAME=metabase
      - MB_DB_PORT=5432
      - MB_DB_USER=metabase
      - MB_DB_PASS=${METABASE_DB_PASSWORD}
      - MB_DB_HOST=metabase-db
      - MB_PLUGINS_DIR=/home/plugins/
    networks:
      - music-tracker
    depends_on:
      - metabase-db
    restart: unless-stopped

  # PostgreSQL for Metabase metadata
  metabase-db:
    image: postgres:15-alpine
    container_name: music-tracker-metabase-db
    environment:
      - POSTGRES_DB=metabase
      - POSTGRES_USER=metabase
      - POSTGRES_PASSWORD=${METABASE_DB_PASSWORD}
    volumes:
      - metabase-db-data:/var/lib/postgresql/data
    networks:
      - music-tracker
    restart: unless-stopped

  # Prefect for orchestration (internal access)
  prefect-server:
    image: prefecthq/prefect:3.4.0-python3.11
    container_name: music-tracker-prefect
    ports:
      - "4200:4200"  # Direct access for internal use
    environment:
      - PREFECT_API_URL=http://localhost:4200/api
      - PREFECT_SERVER_API_HOST=0.0.0.0
      - PREFECT_SERVER_API_PORT=4200
      - PREFECT_API_DATABASE_CONNECTION_URL=postgresql+asyncpg://prefect:${PREFECT_DB_PASSWORD}@prefect-db:5432/prefect
      - PREFECT_SERVER_ANALYTICS_ENABLED=false
    volumes:
      - prefect-data:/opt/prefect
    networks:
      - music-tracker
    depends_on:
      - prefect-db
    restart: unless-stopped
    command: prefect server start --host 0.0.0.0 --port 4200

  prefect-worker:
    build: 
      context: .
      dockerfile: Dockerfile
    container_name: music-tracker-prefect-worker
    environment:
      - PREFECT_API_URL=http://prefect-server:4200/api
      - PREFECT_WORK_POOL_NAME=default-agent-pool
      - SPOTIFY_CLIENT_ID=${SPOTIFY_CLIENT_ID}
      - SPOTIFY_CLIENT_SECRET=${SPOTIFY_CLIENT_SECRET}
      - DUCKDB_PATH=/app/data/music_tracker.duckdb
      - LOG_LEVEL=INFO
    volumes:
      - ./data:/app/data
      - ./logs:/app/logs
      - ./flows:/app/flows
    networks:
      - music-tracker
    depends_on:
      - prefect-server
    restart: unless-stopped
    command: >
      bash -c "
        echo 'Waiting for Prefect server to be ready...';
        until curl -f http://prefect-server:4200/api/health > /dev/null 2>&1; do
          echo 'Prefect server not ready, waiting...';
          sleep 5;
        done;
        echo 'Prefect server is ready!';
        sleep 5;
        echo 'Starting Prefect worker...';
        prefect worker start --pool default-agent-pool
      "

  prefect-deployer:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: music-tracker-prefect-deployer
    environment:
      - PREFECT_API_URL=http://prefect-server:4200/api
      - PREFECT_WORK_POOL_NAME=default-agent-pool
      - SPOTIFY_CLIENT_ID=${SPOTIFY_CLIENT_ID}
      - SPOTIFY_CLIENT_SECRET=${SPOTIFY_CLIENT_SECRET}
      - DUCKDB_PATH=/app/data/music_tracker.duckdb
      - LOG_LEVEL=INFO
    volumes:
      - ./data:/app/data
      - ./logs:/app/logs
      - ./flows:/app/flows
    networks:
      - music-tracker
    depends_on:
      - prefect-worker
    restart: "no"
    command: >
      bash -c "
        echo 'Waiting for Prefect server to be ready...';
        until curl -f http://prefect-server:4200/api/health > /dev/null 2>&1; do
          echo 'Prefect server not ready, waiting...';
          sleep 5;
        done;
        echo 'Prefect server is ready!';
        echo 'Waiting for worker to be ready...';
        sleep 15;
        echo 'Deploying flows...';
        cd /app && PYTHONPATH=/app uv run python flows/orchestrate/deploy_flows.py;
        echo 'Flow deployment completed!';
      "

  # PostgreSQL for Prefect
  prefect-db:
    image: postgres:15-alpine
    container_name: music-tracker-prefect-db
    environment:
      - POSTGRES_DB=prefect
      - POSTGRES_USER=prefect
      - POSTGRES_PASSWORD=${PREFECT_DB_PASSWORD}
    volumes:
      - prefect-db-data:/var/lib/postgresql/data
    networks:
      - music-tracker
    restart: unless-stopped

volumes:
  metabase-data:
  metabase-db-data:
  prefect-data:
  prefect-db-data:

networks:
  music-tracker:
    driver: bridge
EOF

# Create environment file
cat > .env << 'EOF'
SPOTIFY_CLIENT_ID=${SPOTIFY_CLIENT_ID}
SPOTIFY_CLIENT_SECRET=${SPOTIFY_CLIENT_SECRET}
METABASE_DB_PASSWORD=${METABASE_DB_PASSWORD}
PREFECT_DB_PASSWORD=${PREFECT_DB_PASSWORD}
EOF

# Set permissions
chown -R ubuntu:ubuntu /opt/music-tracker

# Create systemd service for automatic startup
cat > /etc/systemd/system/music-tracker.service << 'EOF'
[Unit]
Description=Music Tracker Application
Requires=docker.service
After=docker.service

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=/opt/music-tracker
ExecStart=/usr/local/bin/docker-compose -f docker-compose.internal.yml up -d
ExecStop=/usr/local/bin/docker-compose -f docker-compose.internal.yml down
TimeoutStartSec=0
User=ubuntu
Group=ubuntu

[Install]
WantedBy=multi-user.target
EOF

# Create backup cron job
cat > /etc/cron.d/music-tracker-backup << 'EOF'
# Run backup daily at 3 AM
0 3 * * * root /opt/music-tracker/scripts/backup.sh >> /var/log/music-tracker-backup.log 2>&1
EOF

# Enable services
systemctl enable music-tracker
systemctl daemon-reload

# Install log rotation
cat > /etc/logrotate.d/music-tracker << 'EOF'
/opt/music-tracker/logs/*.log {
    daily
    rotate 7
    compress
    delaycompress
    missingok
    notifempty
    copytruncate
}
EOF

# Create basic health check script
cat > /opt/music-tracker/scripts/health_check.sh << 'EOF'
#!/bin/bash
# Basic health check for Music Tracker services

echo "=== Music Tracker Health Check ==="
echo "Date: $(date)"
echo ""

# Check container status
echo "=== Container Status ==="
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" --filter "name=music-tracker"
echo ""

# Check disk usage
echo "=== Disk Usage ==="
df -h /opt/music-tracker
echo ""

# Check database sizes
echo "=== Database Sizes ==="
if [ -f "/opt/music-tracker/data/music_tracker.duckdb" ]; then
    du -sh /opt/music-tracker/data/music_tracker.duckdb
fi

# Check if services are responding
echo "=== Service Health ==="
curl -s http://localhost:3000/api/health && echo "Metabase: OK" || echo "Metabase: ERROR"
curl -s http://localhost:4200/api/health && echo "Prefect: OK" || echo "Prefect: ERROR"

echo ""
echo "Health check completed."
EOF

chmod +x /opt/music-tracker/scripts/health_check.sh

echo "Setup completed successfully!"
echo ""
echo "To deploy your application:"
echo "1. Copy your application files to /opt/music-tracker"
echo "2. Start the application: sudo systemctl start music-tracker"
echo "3. Check status: sudo systemctl status music-tracker"
echo ""
echo "Access URLs (replace with your server IP):"
echo "- Metabase: http://[SERVER_IP]:3000"
echo "- Prefect: http://[SERVER_IP]:4200"
echo ""
echo "Management commands:"
echo "- Health check: /opt/music-tracker/scripts/health_check.sh"
echo "- View logs: docker-compose -f /opt/music-tracker/docker-compose.internal.yml logs"
echo "- Manual backup: /opt/music-tracker/scripts/backup.sh"