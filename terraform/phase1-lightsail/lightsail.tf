# Create SSH key pair
resource "aws_lightsail_key_pair" "music_tracker" {
  name = "music-tracker-kp"
}

# Lightsail instance
resource "aws_lightsail_instance" "music_tracker" {
  name              = "music-tracker-instance"
  availability_zone = "${var.aws_region}a"
  blueprint_id      = var.instance_blueprint
  bundle_id         = var.instance_bundle
  key_pair_name     = aws_lightsail_key_pair.music_tracker.name

  user_data = templatefile("${path.module}/user_data_internal.sh", {
    SPOTIFY_CLIENT_ID     = var.spotify_client_id
    SPOTIFY_CLIENT_SECRET = var.spotify_client_secret
    METABASE_DB_PASSWORD  = var.metabase_db_password
    PREFECT_DB_PASSWORD   = var.prefect_db_password
  })

  tags = {
    Environment = "production"
    Application = "music-tracker"
  }
}

# Static IP for the instance
resource "aws_lightsail_static_ip" "music_tracker" {
  name = "music-tracker-static-ip"
}

# Attach static IP to instance
resource "aws_lightsail_static_ip_attachment" "music_tracker" {
  static_ip_name = aws_lightsail_static_ip.music_tracker.name
  instance_name  = aws_lightsail_instance.music_tracker.name
}

# Firewall rules
resource "aws_lightsail_instance_public_ports" "music_tracker" {
  instance_name = aws_lightsail_instance.music_tracker.name

  port_info {
    protocol  = "tcp"
    from_port = 22
    to_port   = 22
    cidrs     = ["0.0.0.0/0"]  # SSH access
  }

  port_info {
    protocol  = "tcp"
    from_port = 80
    to_port   = 80
    cidrs     = ["0.0.0.0/0"]  # HTTP
  }

  port_info {
    protocol  = "tcp"
    from_port = 443
    to_port   = 443
    cidrs     = ["0.0.0.0/0"]  # HTTPS
  }
}