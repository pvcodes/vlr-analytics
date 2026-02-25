terraform {
  backend "gcs" {
    bucket = "vlr-analytics-tf-state"
    prefix = "cloud-run-job"
  }
}
