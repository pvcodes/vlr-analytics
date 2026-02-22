resource "google_storage_bucket" "bronze" {
  name                        = var.bronze_bucket_name
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true

  lifecycle_rule {
    condition { age = 180 }
    action { type = "Delete" }
  }
}

# resource "google_storage_bucket" "silver" {
#   name                        = var.silver_bucket_name
#   location                    = var.region
#   force_destroy               = true
#   uniform_bucket_level_access = true
# }

# resource "google_storage_bucket" "gold" {
#   name                        = var.gold_bucket_name
#   location                    = var.region
#   force_destroy               = true
#   uniform_bucket_level_access = true
# }
