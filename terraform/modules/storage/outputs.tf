output "bronze_bucket_name" {
  description = "Name of the bronze GCS bucket"
  value       = google_storage_bucket.bronze.name
}

output "sql_instance_connection_name" {
  description = "Cloud SQL instance connection name for Auth Proxy"
  value       = google_sql_database_instance.events.connection_name
}

output "vlr_events_metadata_sql_instance_name" {
  description = "Cloud SQL instance name"
  value       = google_sql_database_instance.events.name
}
