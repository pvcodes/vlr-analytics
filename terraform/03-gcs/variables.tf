variable "project_id" {
  description = "Google Cloud Project ID"
}
variable "region" {
  description = "Google Cloud Base Region"
  default     = "asia-south1"
}
variable "datalake_bucket_name" {
  description = "VLR Analytics Data lake bucket name"
}

variable "code_bucket_name" {
  description = "VLR Analytics Code bucket name"
}

variable "public_dataset_bucket" {
  description = "VLR Analytics Public Dataset name"
}

variable "public_dataset_bucket_region" {
  description = "VLR Analytics Public Dataset region name"

}
