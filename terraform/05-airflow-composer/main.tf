provider "google-beta" {
  project = var.project_id
  region  = var.region
}

resource "google_project_service" "enable_composer_service" {
  provider = google-beta
  project  = var.project_id
  service  = "composer.googleapis.com"

  disable_on_destroy                    = false
  check_if_service_has_usage_on_destroy = true
}

resource "google_service_account" "composer_worker_sa" {
  provider     = google-beta
  account_id   = "airflow-composer"
  display_name = "Composer Worker Service Account"
}

resource "google_project_iam_member" "composer_worker_role_binding" {
  provider = google-beta
  project  = var.project_id
  member   = format("serviceAccount:%s", google_service_account.composer_worker_sa.email)
  role     = "roles/composer.worker"
}

resource "google_composer_environment" "airflow_composer_env" {
  provider = google-beta
  name     = var.service_name

  config {

    software_config {
      image_version = var.image_version
    }

    node_config {
      service_account = google_service_account.composer_worker_sa.email
    }

  }

  depends_on = [
    google_project_iam_member.composer_worker_role_binding
  ]
}
