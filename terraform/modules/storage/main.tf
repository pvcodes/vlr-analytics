resource "google_storage_bucket" "bronze" {
  name                        = var.bronze_bucket_name
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true

  lifecycle_rule {
    condition { age = 180 }
    action { type = "Delete" }
  }
}

# ── Cloud SQL (Events Source DB) ──────────────────────────────────────────────

resource "google_sql_database_instance" "events" {
  name             = var.vlr_events_metadata_sql_instance_name
  database_version = "POSTGRES_15"
  region           = var.region

  settings {
    tier = "db-g1-small"

    ip_configuration {
      ipv4_enabled = true # public IP
    }

    backup_configuration {
      enabled    = true
      start_time = "03:00"
    }

    maintenance_window {
      day  = 7 # Sunday
      hour = 4
    }
  }

  deletion_protection = false
}

resource "google_sql_database" "events" {
  name     = "events"
  instance = google_sql_database_instance.events.name
}

resource "google_sql_user" "events" {
  name     = var.vlr_events_metadata_sql_user
  instance = google_sql_database_instance.events.name
  password = var.vlr_events_metadata_sql_password
}
