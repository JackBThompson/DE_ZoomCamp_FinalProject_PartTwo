##Objective: Prints key resource identifiers after terraform apply so you know what was created.##

# Output: GCS bucket name
# Output: BigQuery dataset ID
# Output: VM external IP address
# Output: Service account email

# Print key resource details after terraform apply

output "bucket_name" {
  description = "GCS bucket name"
  value       = google_storage_bucket.nba_bucket.name
}

output "bigquery_dataset" {
  description = "BigQuery dataset ID"
  value       = google_bigquery_dataset.nba_dataset.dataset_id
}

output "airflow_vm_ip" {
  description = "External IP of the Airflow VM"
  value       = google_compute_instance.airflow_vm.network_interface[0].access_config[0].nat_ip
}

output "service_account_email" {
  description = "Service account email used by the pipeline"
  value       = google_service_account.nba_pipeline_sa.email
}