terraform {
  required_providers {
    google = { source = "hashicorp/google", version = "~> 5.0" }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_storage_bucket" "data_lake" {
  name          = "${var.project_id}-breastfeeding-lake"
  location      = var.region
  force_destroy = true
}

resource "google_bigquery_dataset" "breastfeeding" {
  dataset_id = "breastfeeding_analytics"
  location   = var.region
}
