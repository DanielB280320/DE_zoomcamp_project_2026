variable "credentials" {
  description = "My credentials"
  default     = "/home/daniel/de_zoomcamp_2026_project/gcs_credentials/credentials/service_account_creds.json"
}

variable "project" {
  description = "Project name"
  default     = "project-0c3c5223-416f-4242-b0f"
}

variable "region" {
  description = "Region"
  default     = "us-central1"
}

variable "location" {
  description = "Project location"
  default     = "US"
}

variable "gcs_storage_class" {
  description = "Bucket storage class"
  default     = "STANDARD"
}

variable "gcs_bucket_name" {
  description = "Bucket name"
  default     = "de-zoomcamp-2026-project-bucket"
}

variable "bq_dataset_name" {
  description = "Bigquery dataset"
  default     = "us_housing_dataset"
}

