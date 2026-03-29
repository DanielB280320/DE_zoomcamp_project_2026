terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.14.1"
    }
  }
}

provider "google" {
  # Configuration options
  credentials = file(var.credentials)
  project     = var.project
  region      = var.region
}

# Specify what kind of resource we want to create
resource "google_storage_bucket" "project_bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

# resource "google_bigquery_dataset" "dataset" {
#   dataset_id = var.bq_dataset_name
#   location   = var.location
# }
