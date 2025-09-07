#!/bin/bash
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

# Install Nginx for reverse proxy
apt-get install -y nginx certbot python3-certbot-nginx

# Create application directory
mkdir -p /opt/music-tracker
cd /opt/music-tracker

# Clone the repository (you'll need to update this with your repo URL)
# For now, we'll create the necessary files directly
cat > docker-compose.prod.yml << 'EOF'
services:
  # Nginx reverse proxy
  nginx:
    image: nginx:alpine
    container_name: music-tracker-nginx
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf
      - /etc/letsencrypt:/etc/letsencrypt
      - /var/www/certbot:/var/www/certbot
    networks:
      - music-tracker
    restart: unless-stopped
    depends_on:
      - metabase
      - prefect-server

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

  # Metabase for reporting
  metabase:
    build:
      context: .
      dockerfile: metabase.Dockerfile
    container_name: music-tracker-metabase
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

  # Prefect for orchestration
  prefect-server:
    image: prefecthq/prefect:3.4.0-python3.11
    container_name: music-tracker-prefect
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

# Create Nginx configuration
cat > nginx.conf << 'EOF'
events {
    worker_connections 1024;
}

http {
    upstream metabase {
        server metabase:3000;
    }
    
    upstream prefect {
        server prefect-server:4200;
    }

    # Redirect HTTP to HTTPS
    server {
        listen 80;
        server_name ${domain_name} ${subdomain}.${domain_name} ${admin_subdomain}.${domain_name};
        
        # Let's Encrypt challenge
        location /.well-known/acme-challenge/ {
            root /var/www/certbot;
        }
        
        # Redirect all other traffic to HTTPS
        location / {
            return 301 https://$server_name$request_uri;
        }
    }

    # Main music tracker site (Metabase)
    server {
        listen 443 ssl http2;
        server_name ${subdomain}.${domain_name};

        ssl_certificate /etc/letsencrypt/live/${subdomain}.${domain_name}/fullchain.pem;
        ssl_certificate_key /etc/letsencrypt/live/${subdomain}.${domain_name}/privkey.pem;

        location / {
            proxy_pass http://metabase;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
        }
    }

    # Admin site (Prefect UI)
    server {
        listen 443 ssl http2;
        server_name ${admin_subdomain}.${domain_name};

        ssl_certificate /etc/letsencrypt/live/${admin_subdomain}.${domain_name}/fullchain.pem;
        ssl_certificate_key /etc/letsencrypt/live/${admin_subdomain}.${domain_name}/privkey.pem;

        # Basic authentication for security
        auth_basic "Admin Access";
        auth_basic_user_file /etc/nginx/.htpasswd;

        location / {
            proxy_pass http://prefect;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
        }
    }
}
EOF

# Create environment file
cat > .env << 'EOF'
SPOTIFY_CLIENT_ID=${spotify_client_id}
SPOTIFY_CLIENT_SECRET=${spotify_client_secret}
METABASE_DB_PASSWORD=${metabase_db_password}
PREFECT_DB_PASSWORD=${prefect_db_password}
EOF

# Create basic auth for admin access
apt-get install -y apache2-utils
echo "admin:$(openssl passwd -apr1 'admin123')" > /etc/nginx/.htpasswd

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
ExecStart=/usr/local/bin/docker-compose -f docker-compose.prod.yml up -d
ExecStop=/usr/local/bin/docker-compose -f docker-compose.prod.yml down
TimeoutStartSec=0
User=ubuntu
Group=ubuntu

[Install]
WantedBy=multi-user.target
EOF

# Enable and start services
systemctl enable nginx
systemctl enable music-tracker
systemctl daemon-reload

# Start nginx
systemctl start nginx

echo "Setup complete. You'll need to:"
echo "1. Copy your application code to /opt/music-tracker"
echo "2. Obtain SSL certificates with: certbot --nginx -d ${subdomain}.${domain_name} -d ${admin_subdomain}.${domain_name}"
echo "3. Start the application with: systemctl start music-tracker"