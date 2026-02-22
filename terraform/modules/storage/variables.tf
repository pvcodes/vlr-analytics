variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
}

variable "bronze_bucket_name" {
  description = "Name of the bronze GCS bucket"
  type        = string
}

# variable "silver_bucket_name" {
#   description = "Name of the silver GCS bucket"
#   type        = string
# }

# variable "gold_bucket_name" {
#   description = "Name of the gold GCS bucket"
#   type        = string
# }
