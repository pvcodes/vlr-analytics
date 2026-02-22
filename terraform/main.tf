terraform {
  backend "gcs" {
    bucket = "vct-analytics-tfstate"
    prefix = "terraform/state"
  }

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

module "apis" {
  source     = "./modules/apis"
  project_id = var.project_id
}

module "storage" {
  source             = "./modules/storage"
  project_id         = var.project_id
  region             = var.region
  bronze_bucket_name = var.bronze_bucket_name
  vlr_events_metadata_sql_instance_name  = var.vlr_events_metadata_sql_instance_name
  vlr_events_metadata_sql_user           = var.vlr_events_metadata_sql_user
  vlr_events_metadata_sql_password       = var.vlr_events_metadata_sql_password
  depends_on         = [module.apis]
}

module "artifact_registry" {
  source     = "./modules/artifact_registry"
  project_id = var.project_id
  region     = var.region
  depends_on = [module.apis]
}

module "iam" {
  source         = "./modules/iam"
  project_id     = var.project_id
  project_number = var.project_number
  region         = var.region
  repository_id  = module.artifact_registry.repository_id
  depends_on     = [module.apis, module.artifact_registry]
}

module "composer" {
  source                       = "./modules/composer"
  project_id                   = var.project_id
  region                       = var.region
  sql_instance_connection_name = module.storage.sql_instance_connection_name
  vlr_events_metadata_sql_user                     = var.vlr_events_metadata_sql_user
  depends_on                   = [module.apis, module.storage]
}
