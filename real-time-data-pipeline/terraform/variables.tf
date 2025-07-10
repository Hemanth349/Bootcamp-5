# variables.tf

variable "project_id" {
  description = "The GCP project ID"
  type        = string
  default     = "ancient-cortex-465315-t4"
}

variable "region" {
  description = "The GCP region to deploy resources in"
  type        = string
  default     = "us-central1"
}



