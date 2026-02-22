variable "project_id" {
  type        = string
  description = "GCP Project ID"
}

variable "project_number" {
  type        = string
  description = "GCP Project Number"
}

variable "region" {
  type        = string
  description = "GCP Region"
  default     = "asia-south1"
}

variable "bronze_bucket_name" {
  type        = string
  description = "GCS bucket for raw/bronze data"
}

variable "silver_bucket_name" {
  type        = string
  description = "GCS bucket for cleaned/silver data"
}

variable "gold_bucket_name" {
  type        = string
  description = "GCS bucket for analytical data"
}

variable "vlr_events_metadata_sql_instance_name" {
  description = "Cloud SQL instance name"
  type        = string
}

variable "vlr_events_metadata_sql_user" {
  description = "Cloud SQL database user"
  type        = string
  default     = "airflow"
}

variable "vlr_events_metadata_sql_password" {
  description = "Cloud SQL database password"
  type        = string
  sensitive   = true
}
