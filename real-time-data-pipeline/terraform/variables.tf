# variables.tf

variable "project_id" {
  description = "The GCP project ID"
  type        = string
  default     = "project-2-465522"
}

variable "region" {
  description = "The GCP region to deploy resources in"
  type        = string
  default     = "us-central1"
}




