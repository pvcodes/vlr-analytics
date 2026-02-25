terraform {
  backend "gcs" {
    bucket = "vlr-analytics-tf-state"
    prefix = "airflow-composer"
  }
}
