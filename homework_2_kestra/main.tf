terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "4.72.1"
    }
  }
}

provider "google" {
  # Configuration options
  # credentials = set up your environment variable GOOGLE_APPLICATION_CREDENTIALS to point to your service account key file using cli variable to credential json file for bucket keys. 
  project     = "dataproject-484804"
  region      = "us-central1"
}


resource "google_storage_bucket" "demo_bucket" {
  name          = "dataproject-484804-demo-bucket"
  location      = "US"
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 3
    }
    action {
      type = "Delete"
    }
  }

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}