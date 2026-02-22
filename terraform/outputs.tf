output "pipeline_service_account_email" {
  description = "Pipeline service account email"
  value       = module.iam.pipeline_service_account_email
}

output "artifact_registry_repository_id" {
  description = "Artifact Registry repository ID"
  value       = module.artifact_registry.repository_id
}

output "bronze_bucket_name" {
  description = "Name of the bronze GCS bucket"
  value       = module.storage.bronze_bucket_name
}

# output "silver_bucket" {
#   value = google_storage_bucket.silver.name
# }

# output "gold_bucket" {
#   value = google_storage_bucket.gold.name
# }

