resource "google_artifact_registry_repository" "vct_analytics" {
  location      = var.region
  repository_id = "vct-analytics"
  description   = "VCT Analytics Docker images"
  format        = "DOCKER"
}
