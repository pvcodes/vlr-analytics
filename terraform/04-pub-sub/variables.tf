variable "project_id" {
  description = "Google Cloud Project ID"
}
variable "region" {
  description = "Google Cloud Base Region"
  default     = "asia-south1"
}
variable "completion_topic_name" {
  type    = string
  default = "vlr-stats-scraper-completion"
}

variable "completion_subscription_name" {
  type    = string
  default = "vlr-stats-scraper-completion-sub"
}

# Cloud Run Job Service Account
variable "cloud_run_service_account" {
  type = string
}
