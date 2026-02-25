resource "google_project_service" "artifactregistry" {
  service = "artifactregistry.googleapis.com"
}

resource "google_artifact_registry_repository" "docker_repo" {
  location      = var.region
  repository_id = var.repo_name
  description   = "Docker repo for VLR scraping jobs"
  format        = "DOCKER"

  depends_on = [
    google_project_service.artifactregistry
  ]
}
