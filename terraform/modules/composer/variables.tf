variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
}

variable "composer_env_name" {
  description = "Cloud Composer environment name"
  type        = string
  default     = "vct-composer"
}

variable "sql_instance_connection_name" {
  description = "Cloud SQL instance connection name"
  type        = string
}

variable "vlr_events_metadata_sql_user" {
  description = "Cloud SQL database user"
  type        = string
}