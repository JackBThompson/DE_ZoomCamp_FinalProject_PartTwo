## Objective: Defines all GCP resources — GCS buckets, BigQuery datasets, GCE VM, and service accounts.

# Configure the GCP provider
terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials_file)
  project     = var.project_id
  region      = var.region
}

# GCS Bucket — raw and processed zones for the data lake
resource "google_storage_bucket" "crm_bucket" {
  name          = var.bucket_name
  location      = var.region
  force_destroy = true

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30                            # delete raw files after 30 days to save cost
    }
  }
}

# GCS folders — raw and processed zones
resource "google_storage_bucket_object" "raw_folder" {
  name    = "raw/"
  content = " "
  bucket  = google_storage_bucket.crm_bucket.name
}

resource "google_storage_bucket_object" "processed_folder" {
  name    = "processed/"
  content = " "
  bucket  = google_storage_bucket.crm_bucket.name
}

# BigQuery Dataset — holds all crm_analytics tables
resource "google_bigquery_dataset" "crm_dataset" {
  dataset_id                = var.bigquery_dataset
  location                  = var.region
  description               = "CRM migration pipeline dataset"
  delete_contents_on_destroy = true
}

# Service Account — used by Airflow and Spark to authenticate with GCP
resource "google_service_account" "crm_pipeline_sa" {
  account_id   = "crm-pipeline-sa"
  display_name = "CRM Pipeline Service Account"
}

# IAM — grant Storage Admin role to service account
resource "google_project_iam_member" "storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.crm_pipeline_sa.email}"
}

# IAM — grant BigQuery Admin role to service account
resource "google_project_iam_member" "bigquery_admin" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.crm_pipeline_sa.email}"
}

# IAM — grant Compute Admin role to service account for GCE VM
resource "google_project_iam_member" "compute_admin" {
  project = var.project_id
  role    = "roles/compute.admin"
  member  = "serviceAccount:${google_service_account.crm_pipeline_sa.email}"
}

# Allow the Terraform service account to attach crm-pipeline-sa to the VM
resource "google_service_account_iam_member" "sa_user" {
  service_account_id = google_service_account.crm_pipeline_sa.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:data-migration-4626@data-migration-pipeline4626.iam.gserviceaccount.com"
}

# GCE VM — hosts Airflow
resource "google_compute_instance" "airflow_vm" {
  name         = "airflow-vm"
  machine_type = "e2-standard-2"
  zone         = "${var.region}-a"
  tags         = ["airflow"]

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2204-lts"
      size  = 50
    }
  }

  network_interface {
    network = "default"
    access_config {}
  }

  service_account {
    email  = google_service_account.crm_pipeline_sa.email
    scopes = ["cloud-platform"]
  }

  metadata_startup_script = <<-EOF
    #!/bin/bash
    apt-get update
    apt-get install -y python3-pip docker.io docker-compose
    pip3 install apache-airflow dbt-bigquery
  EOF
}
