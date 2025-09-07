# Note: This assumes you're managing DNS through Route 53
# If you're using a different DNS provider, you'll need to manually configure DNS records

# Data source for existing hosted zone
data "aws_route53_zone" "main" {
  count = var.domain_name != "" ? 1 : 0
  name  = var.domain_name
}

# A record for music subdomain pointing to static IP
resource "aws_route53_record" "music_tracker" {
  count   = var.domain_name != "" ? 1 : 0
  zone_id = data.aws_route53_zone.main[0].zone_id
  name    = var.subdomain
  type    = "A"
  ttl     = 300
  records = [aws_lightsail_static_ip.music_tracker.ip_address]
}

# A record for admin subdomain pointing to static IP
resource "aws_route53_record" "music_tracker_admin" {
  count   = var.domain_name != "" ? 1 : 0
  zone_id = data.aws_route53_zone.main[0].zone_id
  name    = var.admin_subdomain
  type    = "A"
  ttl     = 300
  records = [aws_lightsail_static_ip.music_tracker.ip_address]
}

# Optional: CNAME for www subdomain
resource "aws_route53_record" "music_tracker_www" {
  count   = var.domain_name != "" ? 1 : 0
  zone_id = data.aws_route53_zone.main[0].zone_id
  name    = "www.${var.subdomain}"
  type    = "CNAME"
  ttl     = 300
  records = ["${var.subdomain}.${var.domain_name}"]
}