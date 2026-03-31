variable "credentials" {
  description = "My credentials"
  default     = "/gcs_credentials/service_account_creds.json" # In case dont work add the full path to the file, not the relative one
}

variable "project" {
  description = "Project name"
  default     = "add_your_project_id_here"
}

variable "region" {
  description = "Region"
  default     = "add_your_region_here"
}

variable "location" {
  description = "Project location"
  default     = "add_your_location_here"
}

variable "gcs_storage_class" {
  description = "Bucket storage class"
  default     = "STANDARD"
}

variable "gcs_bucket_name" {
  description = "Bucket name"
  default     = "add_your_bucket_name_here"
}

variable "bq_dataset_name" {
  description = "Bigquery dataset"
  default     = "add_your_bq_dataset_name_here"
}

