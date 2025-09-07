variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "domain_name" {
  description = "Domain name for the application"
  type        = string
}

variable "subdomain" {
  description = "Subdomain for music tracker"
  type        = string
  default     = "music"
}

variable "admin_subdomain" {
  description = "Admin subdomain for Prefect UI"
  type        = string
  default     = "admin"
}

variable "instance_blueprint" {
  description = "Lightsail instance blueprint"
  type        = string
  default     = "ubuntu_22_04"
}

variable "instance_bundle" {
  description = "Lightsail instance bundle"
  type        = string
  default     = "medium_2_0" # 4GB RAM, 2 vCPUs, 80GB SSD
}

variable "spotify_client_id" {
  description = "Spotify API Client ID"
  type        = string
  sensitive   = true
}

variable "spotify_client_secret" {
  description = "Spotify API Client Secret"
  type        = string
  sensitive   = true
}

variable "metabase_db_password" {
  description = "Password for Metabase PostgreSQL database"
  type        = string
  sensitive   = true
}

variable "prefect_db_password" {
  description = "Password for Prefect PostgreSQL database"
  type        = string
  sensitive   = true
}