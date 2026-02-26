terraform {
  backend "gcs" {
    bucket = "vlr-analytics-tf-state"
    prefix = "datalake-gcs"
  }
}
