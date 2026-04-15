## Objective: Stores configurable values like project ID, region, and bucket name so nothing is hardcoded.

variable "project_id" {
  description = "Your GCP project ID"
  type        = string
  default     = "data-migration-pipeline4626"
}

variable "region" {
  description = "GCP region for all resources"
  type        = string
  default     = "us-east4"
}

variable "credentials_file" {
  description = "Path to GCP service account key JSON file"
  type        = string
  default     = "../gcp-key.json"
}

variable "bucket_name" {
  description = "Name of the GCS bucket for raw and processed data"
  type        = string
  default     = "crm-migration-jt-2025"
}

variable "bigquery_dataset" {
  description = "BigQuery dataset name for CRM analytics tables"
  type        = string
  default     = "data_migration_pipeline4626"
}
