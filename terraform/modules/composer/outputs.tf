output "composer_gcs_bucket" {
  description = "GCS bucket backing the Composer environment (for DAGs)"
  value       = google_composer_environment.vct.config[0].dag_gcs_prefix
}

output "composer_airflow_uri" {
  description = "Airflow web UI URI"
  value       = google_composer_environment.vct.config[0].airflow_uri
}
