provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_pubsub_topic" "stream_topic" {
  name = "stream-topic"
}

resource "google_storage_bucket" "raw_data_bucket" {
  name     = "${var.project_id}-raw-data"
  location = var.region
  force_destroy = true
}

resource "google_bigquery_dataset" "processed_dataset" {
  dataset_id = "streaming_output"
  location   = var.region
}

resource "google_bigquery_table" "processed_table" {
  dataset_id = google_bigquery_dataset.processed_dataset.dataset_id
  table_id   = "user_actions"

  schema = file("${path.module}/../bigquery/schema.json")
  deletion_protection = false
}

resource "google_service_account" "dataflow_sa" {
  account_id   = "dataflow-sa"
  display_name = "Dataflow Service Account"
}

resource "google_project_iam_member" "dataflow_pubsub" {
  role   = "roles/pubsub.subscriber"
  member = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

resource "google_project_iam_member" "dataflow_bq" {
  role   = "roles/bigquery.dataEditor"
  member = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

resource "google_project_iam_member" "dataflow_storage" {
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

resource "google_project_iam_member" "dataflow_worker" {
  role   = "roles/dataflow.worker"
  member = "serviceAccount:${google_service_account.dataflow_sa.email}"
}
