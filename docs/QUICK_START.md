# Quick Start Checklist - Music Tracker Deployment

## Pre-Deployment Checklist (15 minutes)

### ✅ Step 1: AWS Account Setup (5 minutes)
- [ ] **Create AWS Account** at [aws.amazon.com](https://aws.amazon.com) (if you don't have one)
- [ ] **Add billing method** (credit card required, costs ~$21/month)
- [ ] **Create IAM user** for Terraform:
  - AWS Console → IAM → Users → Add user
  - Username: `terraform-user`
  - Access type: "Programmatic access"
  - Permissions: "PowerUserAccess" policy
  - **Save Access Key ID and Secret Key**

**❌ You do NOT need to manually create Lightsail resources - Terraform handles this!**

### ✅ Step 2: Install Terraform (2 minutes)
Choose your method:

**Windows (PowerShell as Administrator):**
```powershell
choco install terraform
```

**macOS:**
```bash
brew install terraform
```

**Linux (Ubuntu/Debian):**
```bash
curl -fsSL https://apt.releases.hashicorp.com/gpg | sudo apt-key add -
sudo apt-add-repository "deb [arch=amd64] https://apt.releases.hashicorp.com $(lsb_release -cs) main"
sudo apt-get update && sudo apt-get install terraform
```

**Verify:**
```bash
terraform version
# Should show: Terraform v1.x.x
```

### ✅ Step 3: Configure AWS Credentials (3 minutes)
**Install AWS CLI:**
- Windows: `choco install awscli`
- macOS: `brew install awscli`
- Linux: `sudo apt install awscli`

**Configure:**
```bash
aws configure
# AWS Access Key ID: [paste from Step 1]
# AWS Secret Access Key: [paste from Step 1]  
# Default region name: us-east-1
# Default output format: json
```

### ✅ Step 4: Get Spotify Credentials (5 minutes)
- [ ] Go to [developer.spotify.com](https://developer.spotify.com/dashboard/)
- [ ] Login with your Spotify account
- [ ] Click "Create an App"
- [ ] App name: "Music Tracker" (any name works)
- [ ] App description: "Personal music analytics"
- [ ] **Copy Client ID and Client Secret**

## Deployment Steps (30 minutes)

### ✅ Step 5: Configure Terraform (2 minutes)
```bash
cd terraform/phase1-lightsail
cp terraform.tfvars.example terraform.tfvars
```

**Edit `terraform.tfvars`:**
```hcl
# Leave empty if you don't want DNS setup yet
domain_name = ""

# Paste your Spotify credentials
spotify_client_id = "your_spotify_client_id_here"
spotify_client_secret = "your_spotify_client_secret_here"

# Generate strong passwords (use a password manager)
metabase_db_password = "YourSecurePassword123!"
prefect_db_password = "AnotherSecurePassword456!"
```

### ✅ Step 6: Deploy Infrastructure (5 minutes)
```bash
# Initialize Terraform (one-time setup)
terraform init

# Review what will be created
terraform plan

# Deploy (will take ~3-5 minutes)
terraform apply
# Type: yes
```

**Save the outputs!** Terraform will show you:
- Server IP address
- SSH connection command

### ✅ Step 7: Deploy Application (10 minutes)
```bash
# Create deployment package from your project root
tar -czf music-tracker-app.tar.gz \
  --exclude='.git' \
  --exclude='terraform' \
  --exclude='docs' \
  .

# Upload to server (replace <SERVER_IP> with actual IP)
scp -i ~/.ssh/music-tracker-key.pem music-tracker-app.tar.gz ubuntu@<SERVER_IP>:/opt/music-tracker/

# Connect to server
ssh -i ~/.ssh/music-tracker-key.pem ubuntu@<SERVER_IP>

# Extract application (on server)
cd /opt/music-tracker
tar -xzf music-tracker-app.tar.gz
sudo chown -R ubuntu:ubuntu /opt/music-tracker

# Start application
sudo systemctl start music-tracker
```

### ✅ Step 8: Verify Deployment (5 minutes)
```bash
# Check service status
sudo systemctl status music-tracker

# Check containers
docker ps

# Run health check
/opt/music-tracker/scripts/health_check.sh
```

### ✅ Step 9: Access Your Application (2 minutes)
- **Metabase**: Open `http://<SERVER_IP>:3000` in browser
- **Prefect**: Open `http://<SERVER_IP>:4200` in browser

## Success! 🎉

Your Music Tracker is now running on AWS Lightsail for ~$21/month.

### Next Steps:
1. **Configure Metabase**: Connect to your DuckDB database at `/data/music_tracker.duckdb`
2. **Test Prefect Flows**: Verify your data pipelines are working
3. **Schedule Regular Backups**: Already configured to run daily at 3 AM

### If Something Goes Wrong:
1. **Check logs**: `docker-compose -f /opt/music-tracker/docker-compose.internal.yml logs`
2. **Restart services**: `sudo systemctl restart music-tracker`
3. **Check troubleshooting guide** in `docs/DEPLOYMENT_GUIDE.md`

### Future Cost Optimization:
Once everything is stable, you can migrate to **Hetzner Cloud** to reduce costs to ~$8/month using the Phase 2 Terraform configuration.

---
**Total Time**: ~45 minutes  
**Monthly Cost**: ~$21  
**Savings vs Microsoft Fabric**: 91-96%