variable "project_id" {
  description = "GCP Project ID"
  type        = string
}
variable "project_name" {
  description = "Project suffix for all services in GCP"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "asia-south1"
}

variable "gcp_service_list" {
  description = "The list of APIs necessary for the project"
  type        = list(string)
  default = [
    "cloudresourcemanager.googleapis.com",
    "serviceusage.googleapis.com",
    "storage.googleapis.com",
    "bigquery.googleapis.com",
    "iam.googleapis.com"
  ]
}
