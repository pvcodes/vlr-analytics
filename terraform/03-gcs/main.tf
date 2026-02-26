resource "google_storage_bucket" "datalake" {
  name     = var.datalake_bucket_name
  location = var.region

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  force_destroy = true
}

resource "google_storage_bucket" "codebucket" {
  name     = var.code_bucket_name
  location = var.region

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  force_destroy = true
}



resource "google_storage_bucket_object" "bronze" {
  name    = "bronze/"
  bucket  = google_storage_bucket.datalake.name
  content = " "
}

resource "google_storage_bucket_object" "silver" {
  name    = "silver/"
  bucket  = google_storage_bucket.datalake.name
  content = " "
}

resource "google_storage_bucket_object" "gold" {
  name    = "gold/"
  bucket  = google_storage_bucket.datalake.name
  content = " "
}
