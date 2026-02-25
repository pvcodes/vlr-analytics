resource "google_project_service" "sqladmin" {
  service = "sqladmin.googleapis.com"
}

resource "google_sql_database_instance" "metadata_instance" {
  name             = var.db_instance_name
  database_version = "POSTGRES_14"
  region           = var.region

  settings {
    tier = "db-f1-micro"

    ip_configuration {
      ipv4_enabled = true
    }
  }

  deletion_protection = false

  depends_on = [
    google_project_service.sqladmin
  ]
}

resource "google_sql_database" "metadata_db" {
  name     = var.db_name
  instance = google_sql_database_instance.metadata_instance.name
}

resource "google_sql_user" "airflow_user" {
  name     = var.db_user
  instance = google_sql_database_instance.metadata_instance.name
  password = var.db_password
}
