locals {
  cloud_build_sa = "${var.project_number}@cloudbuild.gserviceaccount.com"
  compute_sa     = "${var.project_number}-compute@developer.gserviceaccount.com"
}

# ── Pipeline Service Account ──────────────────────────────────────────────────

resource "google_service_account" "pipeline" {
  account_id   = "vct-pipeline-sa"
  display_name = "VCT Analytics Pipeline Service Account"
}

resource "google_project_iam_member" "pipeline_bq_admin" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_project_iam_member" "pipeline_storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_project_iam_member" "pipeline_logging" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

# ── Cloud Build IAM ───────────────────────────────────────────────────────────

resource "google_project_iam_member" "cloud_build_logging" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${local.cloud_build_sa}"
}

resource "google_project_iam_member" "cloud_build_storage_viewer" {
  project = var.project_id
  role    = "roles/storage.objectViewer"
  member  = "serviceAccount:${local.cloud_build_sa}"
}

resource "google_project_iam_member" "cloud_build_artifact_writer_project" {
  project = var.project_id
  role    = "roles/artifactregistry.writer"
  member  = "serviceAccount:${local.cloud_build_sa}"
}

resource "google_artifact_registry_repository_iam_member" "cloud_build_artifact_writer_repo" {
  project    = var.project_id
  location   = var.region
  repository = var.repository_id
  role       = "roles/artifactregistry.writer"
  member     = "serviceAccount:${local.cloud_build_sa}"
}

# ── Compute Engine Default SA IAM ─────────────────────────────────────────────

resource "google_project_iam_member" "compute_artifact_registry_writer" {
  project = var.project_id
  role    = "roles/artifactregistry.writer"
  member  = "serviceAccount:${local.compute_sa}"
}

resource "google_project_iam_member" "cloud_run_logging" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${local.compute_sa}"
}

resource "google_project_iam_member" "compute_storage_admin" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${local.compute_sa}"
}