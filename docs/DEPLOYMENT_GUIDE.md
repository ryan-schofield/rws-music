# Music Tracker Deployment Guide - Phase 1 (AWS Lightsail)

## Overview

This guide covers the deployment of your Music Tracker application to AWS Lightsail for internal use. The deployment includes:
- Containerized application stack with Docker Compose
- DuckDB database for analytics data
- Metabase for reporting (internal access)
- Prefect for orchestration (internal access)
- Automated backup and basic monitoring

**Estimated Deployment Time:** 30-60 minutes  
**Monthly Cost:** ~$21/month

## Prerequisites

### 1. AWS Account Setup

**You'll need an AWS account, but you don't need to manually set up Lightsail - Terraform will handle that.**

1. **Create AWS Account** (if you don't have one):
   - Go to [aws.amazon.com](https://aws.amazon.com)
   - Click "Create an AWS Account"
   - Complete registration and billing setup
   - **Note**: You'll need a credit card, but costs will be ~$21/month

2. **Create IAM User for Terraform**:
   - Go to AWS Console > IAM > Users
   - Click "Add user"
   - Username: `terraform-user`
   - Access type: "Programmatic access"
   - Permissions: Attach existing policy "PowerUserAccess"
   - **Save the Access Key ID and Secret Access Key**

### 2. Install Terraform

**Windows:**
```powershell
# Using Chocolatey (recommended)
choco install terraform

# OR download directly
# 1. Go to https://www.terraform.io/downloads.html
# 2. Download Windows 64-bit
# 3. Extract to C:\terraform\
# 4. Add C:\terraform to your PATH
```

**macOS:**
```bash
# Using Homebrew (recommended)
brew install terraform

# OR using MacPorts
sudo port install terraform
```

**Linux (Ubuntu/Debian):**
```bash
# Add HashiCorp repository
curl -fsSL https://apt.releases.hashicorp.com/gpg | sudo apt-key add -
sudo apt-add-repository "deb [arch=amd64] https://apt.releases.hashicorp.com $(lsb_release -cs) main"

# Install Terraform
sudo apt-get update && sudo apt-get install terraform
```

**Verify Installation:**
```bash
terraform version
# Should show: Terraform v1.x.x
```

### 3. Configure AWS Credentials

**Option 1: AWS CLI (Recommended)**
```bash
# Install AWS CLI
# Windows: choco install awscli
# macOS: brew install awscli
# Linux: sudo apt install awscli

# Configure credentials
aws configure
# AWS Access Key ID: [Enter your key from step 1]
# AWS Secret Access Key: [Enter your secret from step 1]
# Default region name: us-east-1
# Default output format: json
```

**Option 2: Environment Variables**
```bash
# Windows (PowerShell)
$env:AWS_ACCESS_KEY_ID="your-access-key"
$env:AWS_SECRET_ACCESS_KEY="your-secret-key"

# macOS/Linux (Bash)
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
```

### 4. Spotify API Credentials
- Go to [developer.spotify.com](https://developer.spotify.com/dashboard/)
- Create new app (any name/description)
- Note your Client ID and Client Secret

### 5. Application Code
- Your Music Tracker repository code
- Git installed (for code transfer)

## Deployment Steps

### Step 1: Prepare Terraform Configuration

1. **Navigate to terraform directory:**
   ```bash
   cd terraform/phase1-lightsail
   ```

2. **Copy and configure variables:**
   ```bash
   cp terraform.tfvars.example terraform.tfvars
   ```

3. **Edit `terraform.tfvars`:**
   ```hcl
   # Required variables
   domain_name           = "yourdomain.com"  # Optional, leave empty if not using
   spotify_client_id     = "your_spotify_client_id"
   spotify_client_secret = "your_spotify_client_secret"
   metabase_db_password  = "secure_random_password_1"
   prefect_db_password   = "secure_random_password_2"
   
   # Optional overrides
   aws_region = "us-east-1"
   subdomain = "music"
   admin_subdomain = "admin"
   ```

### Step 2: Deploy Infrastructure

1. **Initialize Terraform:**
   ```bash
   terraform init
   ```

2. **Review the deployment plan:**
   ```bash
   terraform plan
   ```

3. **Deploy the infrastructure:**
   ```bash
   terraform apply
   ```
   
4. **Note the outputs:** Save the instance IP address and SSH key information.

### Step 3: Prepare Application Code

1. **Create deployment package:**
   ```bash
   # From your project root
   tar -czf music-tracker-app.tar.gz \
     --exclude='.git' \
     --exclude='terraform' \
     --exclude='docs' \
     --exclude='*.pyc' \
     --exclude='__pycache__' \
     .
   ```

### Step 4: Deploy Application

#### For Windows Users

**1. Extract SSH Private Key (Required - Missing Step):**
```cmd
# Navigate to terraform directory
cd terraform\phase1-lightsail

# Add SSH key output to terraform configuration (if not already done)
# The key should already be available after running terraform apply

# Extract the private key to your SSH directory
mkdir "%USERPROFILE%\.ssh" 2>nul
terraform output -raw ssh_private_key > "%USERPROFILE%\.ssh\music-tracker-key.pem"
```

**2. Create deployment package:**
```cmd
# From your project root directory (correct Windows syntax)
tar -czf music-tracker-app.tar.gz --exclude=".git" --exclude="terraform" --exclude="docs" --exclude="*.pyc" --exclude="__pycache__" --exclude="music-tracker-app.tar.gz" .
```

**3. Transfer application code:**
```cmd
# Windows SCP command (use actual IP from terraform output)
scp -i "%USERPROFILE%\.ssh\music-tracker-key.pem" music-tracker-app.tar.gz ubuntu@44.238.218.183:/opt/music-tracker/
```

**4. Connect and setup application:**
```cmd
# Connect to server and extract files (Windows SSH command)
ssh -i "%USERPROFILE%\.ssh\music-tracker-key.pem" ubuntu@44.238.218.183 "cd /opt/music-tracker && tar -xzf music-tracker-app.tar.gz && sudo chown -R ubuntu:ubuntu /opt/music-tracker"
```

**5. Start the application:**
```cmd
# Start the music tracker service
ssh -i "%USERPROFILE%\.ssh\music-tracker-key.pem" ubuntu@44.238.218.183 "sudo systemctl start music-tracker"
```

#### For Linux/macOS Users

**1. Connect to the server:**
   ```bash
   # Use the SSH command from terraform output
   ssh -i ~/.ssh/music-tracker-key.pem ubuntu@<SERVER_IP>
   ```

**2. Transfer application code:**
   ```bash
   # From local machine
   scp -i ~/.ssh/music-tracker-key.pem music-tracker-app.tar.gz ubuntu@<SERVER_IP>:/opt/music-tracker/
   ```

**3. Extract and setup application:**
   ```bash
   # On the server
   cd /opt/music-tracker
   tar -xzf music-tracker-app.tar.gz
   sudo chown -R ubuntu:ubuntu /opt/music-tracker
   ```

**4. Start the application:**
   ```bash
   sudo systemctl start music-tracker
   ```

### Step 5: Verify Deployment

#### Windows Commands

1. **Check service status:**
   ```cmd
   ssh -i "%USERPROFILE%\.ssh\music-tracker-key.pem" ubuntu@44.238.218.183 "sudo systemctl status music-tracker"
   ```

2. **Check container status:**
   ```cmd
   ssh -i "%USERPROFILE%\.ssh\music-tracker-key.pem" ubuntu@44.238.218.183 "docker ps"
   ```

3. **Run health check:**
   ```cmd
   ssh -i "%USERPROFILE%\.ssh\music-tracker-key.pem" ubuntu@44.238.218.183 "/opt/music-tracker/scripts/health_check.sh"
   ```

#### Linux/macOS Commands

1. **Check service status:**
   ```bash
   sudo systemctl status music-tracker
   ```

2. **Check container status:**
   ```bash
   docker ps
   ```

3. **Run health check:**
   ```bash
   /opt/music-tracker/scripts/health_check.sh
   ```

#### Secure Access via SSH Tunneling (Recommended)

**Windows SSH Tunneling Commands:**
```cmd
# Create SSH tunnels for secure access (run in separate command windows)
# Tunnel 1: Metabase (localhost:3000 -> server:3000)
ssh -i "%USERPROFILE%\.ssh\music-tracker-key.pem" -L 3000:localhost:3000 -N ubuntu@44.238.218.183

# Tunnel 2: Prefect (localhost:4200 -> server:4200)
ssh -i "%USERPROFILE%\.ssh\music-tracker-key.pem" -L 4200:localhost:4200 -N ubuntu@44.238.218.183
```

**After setting up tunnels, access services securely:**
- **Metabase:** `http://localhost:3000` (via SSH tunnel)
- **Prefect:** `http://localhost:4200` (via SSH tunnel)

**Note:** Keep the SSH tunnel commands running in separate terminal windows while you use the services. Press Ctrl+C to stop the tunnels when done.

### Multi-Machine Access Setup

If you need to access the server from another Windows machine, follow these steps:

#### Step 1: Transfer SSH Private Key Securely
**On your current machine (where Terraform was run):**
```cmd
# The private key is located at:
# %USERPROFILE%\.ssh\music-tracker-key.pem
# Copy this file securely to the other machine
```

**Secure transfer options:**
- **USB Drive**: Copy the file to encrypted USB drive
- **Secure Cloud**: Upload to OneDrive/Google Drive (then delete after downloading)
- **Network Copy**: Use secure network copy if machines are on same network

#### Step 2: Setup SSH Directory on New Machine
**On the new Windows machine:**
```cmd
# Create SSH directory
mkdir "%USERPROFILE%\.ssh" 2>nul

# Copy the private key file to this location:
# %USERPROFILE%\.ssh\music-tracker-key.pem
```

#### Step 3: Test SSH Connection
```cmd
# Test basic SSH connection
ssh -i "%USERPROFILE%\.ssh\music-tracker-key.pem" ubuntu@44.238.218.183 "echo 'SSH connection successful from new machine!'"
```

#### Step 4: Create SSH Tunnels on New Machine
```cmd
# Start tunnels (same commands as original machine)
start "Metabase Tunnel" ssh -i "%USERPROFILE%\.ssh\music-tracker-key.pem" -L 3000:localhost:3000 -N ubuntu@44.238.218.183
start "Prefect Tunnel" ssh -i "%USERPROFILE%\.ssh\music-tracker-key.pem" -L 4200:localhost:4200 -N ubuntu@44.238.218.183
```

#### Step 5: Access Services
- **Metabase**: http://localhost:3000
- **Prefect**: http://localhost:4200

#### Security Considerations
- **Private Key Security**: Never store the private key in unsecured locations
- **Access Control**: Only copy to machines you control
- **Key Rotation**: Consider regenerating keys periodically for enhanced security
- **File Permissions**: Keep the private key file restricted to your user account
- **Clean Up**: Delete temporary copies from transfer locations (USB, cloud, etc.)

#### Alternative: Generate New SSH Key Pair
If you prefer not to share the same private key:

1. **Generate new key pair** on the second machine
2. **Add the public key** to the server's authorized_keys
3. **Use the new private key** for connections

```cmd
# On new machine - generate new key pair
ssh-keygen -t rsa -b 2048 -f "%USERPROFILE%\.ssh\new-music-tracker-key"

# Copy public key to server (requires existing access)
scp -i "%USERPROFILE%\.ssh\music-tracker-key.pem" "%USERPROFILE%\.ssh\new-music-tracker-key.pub" ubuntu@44.238.218.183:~/

# On server - add new public key
ssh -i "%USERPROFILE%\.ssh\music-tracker-key.pem" ubuntu@44.238.218.183 "cat ~/new-music-tracker-key.pub >> ~/.ssh/authorized_keys && rm ~/new-music-tracker-key.pub"

# Use new key for connections
ssh -i "%USERPROFILE%\.ssh\new-music-tracker-key" ubuntu@44.238.218.183
```

### Step 6: Initial Configuration

#### Metabase Setup
1. Navigate to `http://<SERVER_IP>:3000`
2. Complete initial setup wizard
3. Connect to DuckDB database at `/data/music_tracker.duckdb`
4. Import your existing dashboard configurations

#### Prefect Setup
1. Navigate to `http://<SERVER_IP>:4200`
2. Verify deployed flows are visible
3. Test manual flow execution
4. Configure flow schedules as needed

## Ongoing Maintenance

### Daily Operations

**Automated Tasks:**
- Daily backups at 3 AM (configured via cron)
- Log rotation (configured via logrotate)
- Container health monitoring and restart

**Manual Checks (Weekly):**
```bash
# SSH into server
ssh -i ~/.ssh/music-tracker-key.pem ubuntu@<SERVER_IP>

# Run health check
/opt/music-tracker/scripts/health_check.sh

# Check disk usage
df -h /opt/music-tracker

# Check recent logs
docker-compose -f /opt/music-tracker/docker-compose.internal.yml logs --tail=50
```

### Monthly Operations

1. **Review backup status:**
   ```bash
   ls -la /opt/music-tracker/backups/
   ```

2. **Update system packages:**
   ```bash
   sudo apt update && sudo apt upgrade -y
   ```

3. **Review resource usage:**
   ```bash
   htop
   docker stats
   ```

### Backup and Restore

#### Manual Backup
```bash
# Run backup script
sudo /opt/music-tracker/scripts/backup.sh
```

#### Restore from Backup
```bash
# List available backups
ls -la /opt/music-tracker/backups/

# Restore specific backup
sudo /opt/music-tracker/scripts/restore.sh YYYYMMDD_HHMMSS
```

#### Backup to S3 (Optional)
Add to your backup script:
```bash
# Install AWS CLI
sudo apt install awscli

# Configure AWS credentials
aws configure

# Add to backup.sh
aws s3 sync /opt/music-tracker/backups/ s3://your-backup-bucket/music-tracker/
```

## Troubleshooting

### Common Issues

#### Containers Won't Start
```bash
# Check logs
docker-compose -f /opt/music-tracker/docker-compose.internal.yml logs

# Check disk space
df -h

# Restart services
sudo systemctl restart music-tracker
```

#### Database Connection Issues
```bash
# Check database containers
docker ps | grep postgres

# Check DuckDB file permissions
ls -la /opt/music-tracker/data/music_tracker.duckdb

# Test database connectivity
docker exec -it music-tracker-metabase-db psql -U metabase -d metabase
```

#### Performance Issues
```bash
# Check resource usage
htop
docker stats

# Check container logs for errors
docker logs music-tracker-metabase
docker logs music-tracker-prefect
```

### Emergency Procedures

#### Complete Service Restart
```bash
sudo systemctl stop music-tracker
sudo systemctl start music-tracker
```

#### Container Reset
```bash
cd /opt/music-tracker
docker-compose -f docker-compose.internal.yml down
docker-compose -f docker-compose.internal.yml up -d
```

#### Disaster Recovery
1. **Deploy new infrastructure** using Terraform
2. **Transfer backup files** to new server
3. **Run restore script** with latest backup
4. **Update DNS** to point to new IP address

## Security Considerations

### Network Security
- Only ports 22 (SSH), 3000 (Metabase), and 4200 (Prefect) are exposed
- SSH key-based authentication only
- Internal Docker network isolation

### Application Security
- Database passwords are secured in environment files
- Regular system updates via unattended-upgrades
- Log rotation prevents disk filling attacks

### Recommended Improvements
- **VPN Access:** Set up WireGuard for secure remote access
- **Firewall Rules:** Configure UFW for additional protection
- **SSL Certificates:** Add Let's Encrypt for HTTPS (future public access)

## Migration Path to Phase 2 (Cost Optimization)

When ready to migrate to Hetzner Cloud for cost savings:

1. **Deploy Phase 2 infrastructure** using terraform/phase2-hetzner
2. **Export all data** using backup scripts
3. **Transfer data** to new infrastructure
4. **Update DNS** records
5. **Destroy Phase 1** infrastructure
6. **Cost savings:** ~60% reduction ($21 → $8-10/month)

## Support and Monitoring

### Log Locations
- Application logs: `/opt/music-tracker/logs/`
- System logs: `/var/log/syslog`
- Docker logs: `docker logs <container_name>`

### Key Metrics to Monitor
- Disk usage: Should stay well below 80GB
- Memory usage: Should stay below 3.5GB
- Container health: All containers should be "healthy"
- Backup success: Daily backups should complete successfully

### Getting Help
1. Check this documentation first
2. Review container logs for specific error messages
3. Verify system resources (disk, memory, network)
4. Test connectivity to external APIs (Spotify, MusicBrainz)

---

**Next Steps:** Once deployment is verified and stable, consider implementing public access for Metabase reports and migration to Phase 2 for cost optimization.