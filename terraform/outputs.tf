output "bronze_bucket" {
  value = google_storage_bucket.bronze.name
}

output "silver_bucket" {
  value = google_storage_bucket.silver.name
}

# output "gold_bucket" {
#   value = google_storage_bucket.gold.name
# }

output "service_account_email" {
  value = google_service_account.pipeline.email
}
