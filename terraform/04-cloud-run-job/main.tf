# Your scraper needs permission to write to: gs://vlr-data-lake/bronze/
resource "google_service_account" "run_job_sa" {
  account_id   = "vlr-scraper-sa"
  display_name = "Cloud Run Job Scraper SA"
}

resource "google_project_iam_member" "gcs_writer" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.run_job_sa.email}"
}

resource "google_cloud_run_v2_job" "scraper_job" {
  name     = var.job_name
  location = var.region

  template {
    parallelism = 1
    task_count  = 1
    template {

      service_account = google_service_account.run_job_sa.email
      max_retries     = 1

      containers {

        image = var.image

        resources {
          limits = {
            cpu    = "1"
            memory = "1Gi"
          }
        }

        env {
          name  = "ENVIRONMENT"
          value = "PRODUCTION"
        }

      }
    }
  }

  depends_on = [
    google_project_iam_member.gcs_writer
  ]
}
