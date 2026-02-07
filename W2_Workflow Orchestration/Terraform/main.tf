terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.16.0"
    }
  }
}

provider "google" {
  # Configuration options
  credentials = file("keys/my-creds.json")  
  project = "innate-solution-484912-b5"
  region  = "us-central1"

}

resource "google_storage_bucket" "bucket_with_lifecycle_os" {
  name          = "auto-bucket-os-terraform-26"
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