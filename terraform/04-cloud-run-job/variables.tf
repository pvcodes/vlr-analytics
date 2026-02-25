variable "project_id" {
  description = "Google Cloud Project ID"
}
variable "region" {
  description = "Google Cloud Base Region"
  default     = "asia-south1"
}

variable "image" {
  description = "vlr.gg/stats scrapper function docker image"
}

variable "job_name" {
  description = "Google Cloud Run Job name for vlr.gg scrapper"
}
