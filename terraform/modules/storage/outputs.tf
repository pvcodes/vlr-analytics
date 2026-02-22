output "bronze_bucket_name" {
  description = "Name of the bronze GCS bucket"
  value       = google_storage_bucket.bronze.name
}
