variable "project_id" {
  description = "Google Cloud Project ID"
}
variable "region" {
  description = "Google Cloud Base Region"
  default     = "asia-south1"
}

variable "dataproc_service_account_id" {
  type    = string
  default = "dataproc-serverless-sa"
}
