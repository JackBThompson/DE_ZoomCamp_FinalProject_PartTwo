##Objective: Stores configurable values like project ID, region, and bucket name so nothing is hardcoded.##

# GCP project ID
variable "project_id" {
  description = "Your GCP project ID"
  type        = string
}

# GCP region — all resources in the same region to avoid egress costs
variable "region" {
  description = "GCP region for all resources"
  type        = string
  default     = "us-east4"
}

# Path to your service account key file
variable "credentials_file" {
  description = "Path to GCP service account key JSON file"
  type        = string
  default     = "../gcp-key.json"
}

# GCS bucket name
variable "bucket_name" {
  description = "Name of the GCS bucket for raw and processed data"
  type        = string
}

# BigQuery dataset name
variable "bigquery_dataset" {
  description = "BigQuery dataset name for NBA analytics tables"
  type        = string
  default     = "nba_analytics"
}