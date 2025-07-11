output "pubsub_topic" {
  value = google_pubsub_topic.stream_topic.name
}

output "storage_bucket" {
  value = google_storage_bucket.raw_data_bucket.url
}

output "bigquery_table" {
  value = google_bigquery_table.processed_table.table_id
}

/* 
If the service account resource does not exist currently, comment this output out
output "dataflow_service_account" {
  value = google_service_account.dataflow_sa.email
}
*/
