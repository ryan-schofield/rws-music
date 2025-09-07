output "instance_public_ip" {
  description = "Public IP address of the Lightsail instance"
  value       = aws_lightsail_static_ip.music_tracker.ip_address
}

output "instance_name" {
  description = "Name of the Lightsail instance"
  value       = aws_lightsail_instance.music_tracker.name
}

output "ssh_key_name" {
  description = "Name of the SSH key pair"
  value       = aws_lightsail_key_pair.music_tracker.name
}

output "dns_records" {
  description = "DNS records that need to be configured"
  value = var.domain_name != "" ? {
    music_subdomain = "${var.subdomain}.${var.domain_name} -> ${aws_lightsail_static_ip.music_tracker.ip_address}"
    admin_subdomain = "${var.admin_subdomain}.${var.domain_name} -> ${aws_lightsail_static_ip.music_tracker.ip_address}"
  } : {
    note = "No domain configured. Access via IP: ${aws_lightsail_static_ip.music_tracker.ip_address}"
  }
}

output "application_urls" {
  description = "URLs for accessing the application"
  value = var.domain_name != "" ? {
    metabase = "https://${var.subdomain}.${var.domain_name}"
    prefect  = "https://${var.admin_subdomain}.${var.domain_name}"
  } : {
    metabase = "http://${aws_lightsail_static_ip.music_tracker.ip_address}:3000"
    prefect  = "http://${aws_lightsail_static_ip.music_tracker.ip_address}:4200"
  }
}

output "ssh_connection" {
  description = "SSH connection command"
  value       = "ssh -i ~/.ssh/${aws_lightsail_key_pair.music_tracker.name}.pem ubuntu@${aws_lightsail_static_ip.music_tracker.ip_address}"
}

output "deployment_commands" {
  description = "Commands to run after deployment"
  value = var.domain_name != "" ? [
    "1. SSH into the instance: ssh -i ~/.ssh/${aws_lightsail_key_pair.music_tracker.name}.pem ubuntu@${aws_lightsail_static_ip.music_tracker.ip_address}",
    "2. Copy your application code to /opt/music-tracker",
    "3. Configure SSL certificates: sudo certbot --nginx -d ${var.subdomain}.${var.domain_name} -d ${var.admin_subdomain}.${var.domain_name}",
    "4. Start the application: sudo systemctl start music-tracker",
    "5. Check status: sudo systemctl status music-tracker"
  ] : [
    "1. SSH into the instance: ssh -i ~/.ssh/${aws_lightsail_key_pair.music_tracker.name}.pem ubuntu@${aws_lightsail_static_ip.music_tracker.ip_address}",
    "2. Copy your application code to /opt/music-tracker",
    "3. Start the application: sudo systemctl start music-tracker",
    "4. Check status: sudo systemctl status music-tracker",
    "5. Access Metabase at: http://${aws_lightsail_static_ip.music_tracker.ip_address}:3000",
    "6. Access Prefect at: http://${aws_lightsail_static_ip.music_tracker.ip_address}:4200"
  ]
}

output "ssh_private_key" {
  description = "Private SSH key for connecting to the instance"
  value       = aws_lightsail_key_pair.music_tracker.private_key
  sensitive   = true
}