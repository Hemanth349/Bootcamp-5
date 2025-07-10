terraform {
  backend "gcs" {
    bucket = "your-tf-state-bucket1"
    prefix = "real-time-data-pipeline/terraform/state"
  }
}

