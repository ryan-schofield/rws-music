#!/bin/bash
# Music Tracker Backup Script
# This script backs up DuckDB database and PostgreSQL databases

set -e

# Configuration
BACKUP_DIR="/opt/music-tracker/backups"
DATE=$(date +"%Y%m%d_%H%M%S")
RETENTION_DAYS=30

# Create backup directory
mkdir -p "$BACKUP_DIR"

# Function to log messages
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

log "Starting backup process..."

# Backup DuckDB database
if [ -f "/opt/music-tracker/data/music_tracker.duckdb" ]; then
    log "Backing up DuckDB database..."
    cp "/opt/music-tracker/data/music_tracker.duckdb" "$BACKUP_DIR/music_tracker_${DATE}.duckdb"
    gzip "$BACKUP_DIR/music_tracker_${DATE}.duckdb"
    log "DuckDB backup completed: music_tracker_${DATE}.duckdb.gz"
else
    log "Warning: DuckDB database not found"
fi

# Backup PostgreSQL databases
log "Backing up PostgreSQL databases..."

# Metabase database
docker exec music-tracker-metabase-db pg_dump -U metabase metabase | gzip > "$BACKUP_DIR/metabase_${DATE}.sql.gz"
log "Metabase database backup completed: metabase_${DATE}.sql.gz"

# Prefect database
docker exec music-tracker-prefect-db pg_dump -U prefect prefect | gzip > "$BACKUP_DIR/prefect_${DATE}.sql.gz"
log "Prefect database backup completed: prefect_${DATE}.sql.gz"

# Backup application logs (last 7 days)
if [ -d "/opt/music-tracker/logs" ]; then
    log "Backing up application logs..."
    find /opt/music-tracker/logs -name "*.log" -mtime -7 | tar -czf "$BACKUP_DIR/logs_${DATE}.tar.gz" -T -
    log "Logs backup completed: logs_${DATE}.tar.gz"
fi

# Backup docker-compose configuration
log "Backing up configuration files..."
tar -czf "$BACKUP_DIR/config_${DATE}.tar.gz" -C /opt/music-tracker \
    docker-compose.yml \
    docker-compose.prod.yml \
    .env \
    Dockerfile \
    metabase.Dockerfile 2>/dev/null || true
log "Configuration backup completed: config_${DATE}.tar.gz"

# Clean up old backups
log "Cleaning up backups older than $RETENTION_DAYS days..."
find "$BACKUP_DIR" -name "*.gz" -mtime +$RETENTION_DAYS -delete
find "$BACKUP_DIR" -name "*.sql.gz" -mtime +$RETENTION_DAYS -delete

# Calculate backup size
BACKUP_SIZE=$(du -sh "$BACKUP_DIR" | cut -f1)
log "Backup process completed. Total backup size: $BACKUP_SIZE"

# Optional: Upload to S3 (uncomment and configure if needed)
# aws s3 sync "$BACKUP_DIR" s3://your-backup-bucket/music-tracker/ --delete