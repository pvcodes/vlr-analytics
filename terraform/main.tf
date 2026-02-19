terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Enable required APIs
resource "google_project_service" "gcp_services" {
  for_each = toset(var.gcp_service_list)

  project            = var.project_id
  service            = each.key
  disable_on_destroy = false
}

# GCS Buckets (depend on storage API)
resource "google_storage_bucket" "bronze" {
  name          = "${var.project_name}-bronze-dl"
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 180 # delete raw data after 180 days
    }
    action {
      type = "Delete"
    }
  }

  depends_on = [google_project_service.gcp_services]
}

resource "google_storage_bucket" "silver" {
  name          = "${var.project_name}-silver-dl"
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true

  depends_on = [google_project_service.gcp_services]
}

# resource "google_storage_bucket" "gold" {
#   name          = "${var.project_name}-gold-dl"
#   location      = var.region
#   force_destroy = true

#   uniform_bucket_level_access = true

#   depends_on = [google_project_service.gcp_services]
# }

# BigQuery Datasets (depend on bigquery API)
# resource "google_bigquery_dataset" "staging" {
#   dataset_id  = "vct_staging"
#   location    = var.region
#   description = "Staging layer - raw data from silver"

#   depends_on = [google_project_service.gcp_services]
# }

# resource "google_bigquery_dataset" "intermediate" {
#   dataset_id  = "vct_intermediate"
#   location    = var.region
#   description = "Intermediate transformations"

#   depends_on = [google_project_service.gcp_services]
# }

# resource "google_bigquery_dataset" "marts" {
#   dataset_id  = "vct_marts"
#   location    = var.region
#   description = "Analytics-ready marts"

#   depends_on = [google_project_service.gcp_services]
# }

# Service Account for pipeline (depends on IAM API)
resource "google_service_account" "pipeline" {
  account_id   = "vct-pipeline-sa"
  display_name = "VCT Analytics Pipeline Service Account"

  depends_on = [google_project_service.gcp_services]
}

# Grant permissions
resource "google_project_iam_member" "pipeline_bq_admin" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.pipeline.email}"

  depends_on = [google_service_account.pipeline]
}

resource "google_project_iam_member" "pipeline_storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.pipeline.email}"

  depends_on = [google_service_account.pipeline]
}
