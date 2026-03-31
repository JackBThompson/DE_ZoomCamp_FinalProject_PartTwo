##Objective: Defines all GCP resources — GCS buckets, BigQuery datasets, GCE VM, and service accounts.##

# Create GCS bucket with two folders: raw/ and processed/
# Create BigQuery dataset called "github_analytics"
# Create GCE e2-micro VM to host Airflow
# Create service account with roles: BigQuery Admin, Storage Admin
# Output service account key to local file for use in other services

# Configure the GCP provider — tells Terraform which project and region to use
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
resource "google_storage_bucket" "nba_bucket" {
  name          = var.bucket_name
  location      = var.region
  force_destroy = true                    # allows bucket to be deleted even if it has files

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
  bucket  = google_storage_bucket.nba_bucket.name
}

resource "google_storage_bucket_object" "processed_folder" {
  name    = "processed/"
  content = " "
  bucket  = google_storage_bucket.nba_bucket.name
}

# BigQuery Dataset — holds all nba_analytics tables
resource "google_bigquery_dataset" "nba_dataset" {
  dataset_id = var.bigquery_dataset
  location   = var.region
  description = "NBA analytics pipeline dataset"

  delete_contents_on_destroy = true       # allows dataset to be deleted even if it has tables
}

# Service Account — used by Airflow and Spark to authenticate with GCP
resource "google_service_account" "nba_pipeline_sa" {
  account_id   = "nba-pipeline-sa"
  display_name = "NBA Pipeline Service Account"
}

# IAM — grant Storage Admin role to service account
resource "google_project_iam_member" "storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.nba_pipeline_sa.email}"
}

# IAM — grant BigQuery Admin role to service account
resource "google_project_iam_member" "bigquery_admin" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.nba_pipeline_sa.email}"
}

# IAM — grant Compute Admin role to service account for GCE VM
resource "google_project_iam_member" "compute_admin" {
  project = var.project_id
  role    = "roles/compute.admin"
  member  = "serviceAccount:${google_service_account.nba_pipeline_sa.email}"
}

# GCE VM — hosts Airflow
resource "google_compute_instance" "airflow_vm" {
  name         = "airflow-vm"
  machine_type = "e2-standard-2"          # 2 vCPUs, 8GB RAM — enough for Airflow + Spark
  zone         = "${var.region}-a"
  tags = ["airflow"]

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2204-lts"
      size  = 50                          # 50GB disk
    }
  }

  network_interface {
    network = "default"
    access_config {}                      # gives the VM a public IP
  }

  service_account {
    email  = google_service_account.nba_pipeline_sa.email
    scopes = ["cloud-platform"]           # full GCP access via service account
  }

  metadata_startup_script = <<-EOF
    #!/bin/bash
    apt-get update
    apt-get install -y python3-pip docker.io docker-compose
    pip3 install apache-airflow nba_api
  EOF
}