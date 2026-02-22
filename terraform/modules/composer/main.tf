resource "google_service_account" "composer" {
  account_id   = "vct-composer-sa"
  display_name = "VCT Composer Service Account"
}

resource "google_composer_environment" "vct" {
  name    = var.composer_env_name
  region  = var.region
  project = var.project_id

  config {
    software_config {
      image_version = "composer-2-airflow-2"

      env_variables = {
        ENVIRONMENT                  = "PRODUCTION"
        SQL_INSTANCE_CONNECTION      = var.sql_instance_connection_name
        SQL_DB                       = "events"
        vlr_events_metadata_sql_user = var.vlr_events_metadata_sql_user
      }

      pypi_packages = {
        "apache-airflow-providers-google"   = ""
        "apache-airflow-providers-postgres" = ""
        "cloud-sql-python-connector"        = ""
        "pg8000"                            = ""
      }
    }

    node_config {
      service_account = google_service_account.composer.email
    }
  }
}
