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
  credentials = "/workspaces/data-engineering/homework_2_kestra/keys/cred.json"
  project     = "dataproject-484804"
  region      = "us-central1"
}


# resource "google_storage_bucket" "auto-expire" {
#   name          = "auto-expiring-bucket"
#   location      = "US"
#   force_destroy = true

#   lifecycle_rule {
#     condition {
#       age = 3
#     }
#     action {
#       type = "Delete"
#     }
#   }

#   lifecycle_rule {
#     condition {
#       age = 1
#     }
#     action {
#       type = "AbortIncompleteMultipartUpload"
#     }
#   }
# }