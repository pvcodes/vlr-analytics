terraform {
  backend "gcs" {
    bucket = "vlr-analytics-tf-state"
    prefix = "artifact-registry"
  }
}
