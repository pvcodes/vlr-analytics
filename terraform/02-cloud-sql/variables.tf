variable "project_id" {
  description = "Google Cloud Project ID"
}

variable "region" {
  description = "Google Cloud Base Region"
  default     = "asia-south1"
}

variable "db_instance_name" {
  description = "VLR Anayltics Metadata DB Name"
}
variable "db_name" {
  description = "VLR Events Metadata instance name"
}
variable "db_user" {
  description = "VLR Anayltics Metadata DB username"
}
variable "db_password" {
  description = "VLR Anayltics Metadata DB password"
  sensitive   = true
}
