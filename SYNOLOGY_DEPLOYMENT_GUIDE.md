# Synology NAS Deployment Guide

This guide provides step-by-step instructions for deploying the music tracking application on a Synology NAS with Container Manager.

## Prerequisites

- Synology NAS with DSM 7.0 or later
- Container Manager package installed
- Minimum 2GB RAM (4GB recommended for better performance)
- Docker package installed (if using older DSM versions)
- Admin access to the NAS

## Installation Steps

### 1. Prepare Your Synology NAS

#### 1.1 Install Container Manager
1. Open **Package Center**
2. Search for "Container Manager"
3. Click **Install**
4. Wait for installation to complete

#### 1.2 Create Shared Folder for Docker
1. Open **Control Panel** > **Shared Folder**
2. Click **Create**
3. Name: `docker`
4. Location: Select your preferred volume (SSD recommended)
5. Click **Next** and complete the wizard

#### 1.3 Set Up Project Directory
1. Open **File Station**
2. Navigate to your `docker` shared folder
3. Create a new folder: `music-tracker`
4. Inside `music-tracker`, create these subfolders:
   - `config`
   - `data`
   - `logs`
   - `metabase`

### 2. Transfer Project Files

#### 2.1 Copy Project Files to NAS
1. Copy the entire project directory to your Synology NAS
2. Place it in: `/volume1/docker/music-tracker`
3. Ensure all files are transferred, including:
   - `docker-compose.yml`
   - All configuration files
   - Python scripts and flows

#### 2.2 Set File Permissions
1. Open **Control Panel** > **Shared Folder**
2. Select the `docker` shared folder
3. Click **Edit** > **Permissions**
4. Ensure your user has **Read/Write** permissions
5. Click **Apply**

### 3. Configure Container Manager

#### 3.1 Import Project
1. Open **Container Manager**
2. Go to **Project** tab
3. Click **Create** > **Create from docker-compose.yml**
4. Browse to: `/volume1/docker/music-tracker/docker-compose.yml`
5. Click **Next**

#### 3.2 Configure Environment Variables
1. In the project creation screen, go to **Environment**
2. Add the following variables (or import from `.env.example`):
   ```
   # Spotify API Credentials (Required)
   SPOTIFY_CLIENT_ID=your_spotify_client_id
   SPOTIFY_CLIENT_SECRET=your_spotify_client_secret
   SPOTIFY_REFRESH_TOKEN=your_spotify_refresh_token
   
   # n8n Configuration (Required)
   N8N_HOST=<your-synology-ip>
   TIMEZONE=UTC
   
   # Metabase Configuration  
   MB_DB_TYPE=h2
   MB_DB_FILE=/home/metabase.db
   
   # Data Paths
   DATA_DIR=/data
   DUCKDB_PATH=/data/music_tracker.duckdb
   
   # Memory Limits (already configured in docker-compose.yml)
   ```

#### 3.3 Configure Volume Mappings
1. In the project creation screen, go to **Volume**
2. Verify the following mappings:
   - `/volume1/docker/music-tracker/data` → `/data`
   - `/volume1/docker/music-tracker/dbt` → `/dbt`
   - `/volume1/docker/music-tracker/flows` → `/flows`
   - `/volume1/docker/music-tracker/logs` → `/logs`
   - `/volume1/docker/music-tracker/n8n-workflows` → `/workflows`

### 4. Start the Services

#### 4.1 Deploy the Stack
1. Click **Next** through the remaining screens
2. Review the configuration summary
3. Click **Done** to create the project
4. Click **Start** to launch all containers

#### 4.2 Monitor Startup
1. Go to **Container** tab in Container Manager
2. Monitor the logs for each service:
   - `music-tracker-n8n`
   - `music-tracker-metabase`
   - `music-tracker-pipeline`

### 5. Access the Applications and Deploy Workflows

#### 5.1 Access n8n
1. Open a web browser
2. Navigate to: `http://<your-synology-ip>:5678`
3. Complete the initial setup:
   - Create admin account
   - Configure credentials for API calls
   - Deploy workflows using the CLI script

#### 5.2 Deploy n8n Workflows
1. SSH into your Synology NAS (or use Task Scheduler)
2. Navigate to the project directory:
   ```bash
   cd /volume1/docker/music-tracker
   ```
3. Deploy workflows:
   ```bash
   docker exec music-tracker-pipeline python flows/cli/deploy_n8n_workflows.py --action deploy
   ```
4. Verify workflows were created in n8n UI (check Workflows tab)

#### 5.3 Access Metabase
1. Open a web browser
2. Navigate to: `http://<your-synology-ip>:3000`
3. Complete the initial setup:
   - Create admin account
   - Connect to DuckDB database
   - Import dashboards from `metabase/dashboards.json`

## Storage Optimization

### SSD Cache Configuration

If your Synology NAS has SSD cache:

1. Open **Storage Manager**
2. Go to **SSD Cache**
3. Click **Create**
4. Select the volume containing your `docker` shared folder
5. Choose your SSD cache devices
6. Select **Read/Write** cache mode
7. Click **Next** and complete the wizard

### Backup Strategy

#### Automated Backups
1. Open **Hyper Backup**
2. Click **Create** > **Data backup task**
3. Select **Local folder & USB**
4. Choose destination (external drive or another shared folder)
5. Select source: `/volume1/docker/music-tracker/data`
6. Schedule: Daily at 2:00 AM
7. Enable versioning (keep 7 versions)
8. Click **Apply**

#### Manual Backups
Regularly backup these critical files:
- `/volume1/docker/music-tracker/data/music_tracker.duckdb`
- `/volume1/docker/music-tracker/metabase/metabase.db`
- `/volume1/docker/music-tracker/config`

### Log Rotation

1. Open **Log Center**
2. Go to **Log Rotation**
3. Add rotation rule for Container Manager logs:
   - Log type: **Container Manager**
   - Rotation: **Daily**
   - Keep: **7 days**
4. Click **Apply**

## Monitoring Setup

### Resource Monitor

1. Open **Resource Monitor**
2. Go to **Performance** tab
3. Add widgets for:
   - Memory usage
   - CPU usage
   - Network traffic
4. Set up alerts:
   - Memory > 90% for 5 minutes
   - CPU > 80% for 10 minutes

### Container Manager Notifications

1. Open **Container Manager**
2. Go to **Settings**
3. Enable notifications:
   - Container crashes
   - Memory limits exceeded
   - CPU usage warnings
4. Configure email or mobile notifications

### Log Aggregation

1. Open **Log Center**
2. Go to **Log Forwarding**
3. Add forwarding rule:
   - Source: **Container Manager**
   - Destination: **File** or **Syslog server**
   - Path: `/volume1/docker/music-tracker/logs/container.log`

## Optional: Synology Reverse Proxy

For easier access on your local network:

### 1. Set Up Reverse Proxy
1. Open **Control Panel** > **Application Portal**
2. Go to **Reverse Proxy**
3. Click **Create**

#### n8n Reverse Proxy
- Source:
  - Protocol: **HTTP**
  - Hostname: `n8n.yourdomain.local`
  - Port: **80**
- Destination:
  - Protocol: **HTTP**
  - Hostname: `localhost`
  - Port: **5678**

#### Metabase Reverse Proxy
- Source:
  - Protocol: **HTTP**
  - Hostname: `metabase.yourdomain.local`
  - Port: **80**
- Destination:
  - Protocol: **HTTP**
  - Hostname: `localhost`
  - Port: **3000**

### 2. Configure Custom Domain (Optional)

1. Open **Control Panel** > **Network**
2. Go to **DNS Server**
3. Add local DNS entries:
   - `n8n.yourdomain.local` → `<your-synology-ip>`
   - `metabase.yourdomain.local` → `<your-synology-ip>`

### 3. Update Hosts File (Alternative)

On each device that needs access:
- Windows: `C:\Windows\System32\drivers\etc\hosts`
- macOS/Linux: `/etc/hosts`

Add:
```
<your-synology-ip> n8n.yourdomain.local
<your-synology-ip> metabase.yourdomain.local
```

## Troubleshooting

### Common Issues

#### Memory Limits Exceeded
1. Check **Resource Monitor** for memory usage
2. Adjust memory limits in `docker-compose.yml`
3. Restart affected containers

#### Container Fails to Start
1. Check logs in **Container Manager**
2. Verify volume permissions
3. Ensure all required files are present

#### Metabase Performance Issues
1. Increase JVM heap size in `metabase.Dockerfile`
2. Restart Metabase container
3. Monitor memory usage

### Logs and Debugging

- **Container logs**: Available in Container Manager > Container > Logs
- **Application logs**: `/volume1/docker/music-tracker/logs`
- **Docker events**: `docker events` via SSH

## Maintenance

### Regular Tasks

1. **Weekly**:
   - Check Resource Monitor for anomalies
   - Review Container Manager notifications
   - Verify backups are completing successfully

2. **Monthly**:
   - Update Docker images (pull latest versions)
   - Clean up old logs
   - Test backup restoration

3. **Quarterly**:
   - Review memory usage patterns
   - Optimize DuckDB database
   - Update documentation

### Updating the Application

1. Stop all containers in Container Manager
2. Backup critical data files
3. Update project files on the NAS
4. Rebuild containers if needed
5. Start containers and verify functionality

## Performance Tuning

### Memory Optimization

- Monitor memory usage in **Resource Monitor**
- Adjust `mem_limit` values in `docker-compose.yml` as needed
- Consider increasing Metabase heap size if queries are slow

### Storage Optimization

- Use SSD cache for frequently accessed files
- Regularly compact DuckDB database
- Clean up old log files

### Network Optimization

- Use wired connection for better stability
- Configure QoS if needed
- Monitor network traffic patterns

## Security Considerations

### Access Control

- Restrict access to Synology NAS admin interface
- Use strong passwords for all services
- Consider VPN for remote access

### Data Protection

- Enable encryption for sensitive data
- Regularly backup critical files
- Test backup restoration process

### Network Security

- Keep DSM and packages updated
- Use firewall to restrict access
- Monitor for suspicious activity

## Support

For issues specific to this application:
- Check application logs
- Review this deployment guide
- Consult the main README.md

For Synology-specific issues:
- Synology Knowledge Center
- Synology Community Forum
- Official Synology Support

## Appendix: Useful Commands

### SSH Commands

```bash
# Check running containers
docker ps

# View container logs
docker logs <container-name>

# Check memory usage
docker stats

# Restart a container
docker restart <container-name>

# Access container shell
docker exec -it <container-name> /bin/bash
```

### DSM Commands

- **Restart Container Manager**: Control Panel > Task Scheduler > Create > Triggered Task > Event: "Container Manager service started"
- **Check system logs**: Log Center > System Logs
- **Monitor resources**: Resource Monitor > Performance

## Version History

- **1.0**: Initial Synology deployment guide with Prefect
- **1.1**: Added SSD cache configuration
- **1.2**: Enhanced troubleshooting section
- **1.3**: Added performance tuning tips
- **2.0**: Migrated from Prefect to n8n orchestration, updated all instructions

## License

This deployment guide is provided as-is and is subject to the same license as the main project.