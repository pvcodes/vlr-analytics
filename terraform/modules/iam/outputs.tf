output "pipeline_service_account_email" {
  description = "Pipeline service account email"
  value       = google_service_account.pipeline.email
}
