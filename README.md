# NBA Analytics Pipeline

## Problem Description

NBA fans, analysts, and fantasy sports players can look up a singular player's performance on ESPN. Yet, there is no practical method of comparing multiple players' performance at once and visualizing how multiple player performances evolve over the course of a full season. This pipeline solves this problem by ingesting NBA game and player data from the NBA API, transforming it with PySpark, and surfacing player performance trends in a Looker Studio dashboard.

The project implements a full ETL pipeline:

- **Extract** — raw game and player stats pulled from the NBA API and landed as JSON in Google Cloud Storage
- **Transform** — PySpark batch job cleans, casts, deduplicates, and writes processed data to BigQuery
- **Load** — partitioned and clustered BigQuery tables feed SQL views that power the Looker Studio dashboard

---

## Architecture

```
NBA API
  → Airflow DAG (orchestration)
  → GCS (raw JSON storage)
  → PySpark (transformation)
  → BigQuery (warehouse + SQL views)
  → Looker Studio (dashboard)
```

---

## Tech Stack

| Layer | Tool |
|---|---|
| Cloud | GCP |
| IaC | Terraform |
| Orchestration | Airflow (Docker) |
| Storage | Google Cloud Storage |
| Processing | PySpark |
| Warehouse | BigQuery |
| Dashboard | Looker Studio |

---

## Prerequisites

- [gcloud CLI](https://cloud.google.com/sdk/docs/install)
- [Terraform](https://developer.hashicorp.com/terraform/install)
- Docker + Docker Compose
- Python 3.10+
- Free GCP account with billing enabled

No NBA API key required — `nba_api` is a free library with no authentication.

---

## Setup Instructions

> All steps run locally on your terminal. Step 11 requires SSHing into the GCP VM to start Airflow, but the SSH command is run from your local terminal.
> If you run into any problems reproducing, shoot me an email at **etl.jackt@gmail.com** and I'll help troubleshoot.

### 1. Clone the repo
```bash
git clone https://github.com/JackBThompson/DE_ZoomCamp_FinalProject.git
cd DE_ZoomCamp_FinalProject
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Install Spark 3.5.1
> **Important:** Use Spark 3.5.1 specifically. Later versions use Scala 2.13 which is incompatible with the BigQuery connector.

**Mac:**
```bash
curl -O https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
tar -xzf spark-3.5.1-bin-hadoop3.tgz
```

**Linux:**
```bash
wget https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
tar -xzf spark-3.5.1-bin-hadoop3.tgz
```

Add to `~/.zshrc` (Mac) or `~/.bashrc` (Linux) to make permanent:
```bash
echo 'export SPARK_HOME=$HOME/spark-3.5.1-bin-hadoop3' >> ~/.zshrc
echo 'export PATH=$SPARK_HOME/bin:$PATH' >> ~/.zshrc
source ~/.zshrc
```

### 4. Download the GCS connector JAR

**Mac:**
```bash
curl -O https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar
mv gcs-connector-hadoop3-latest.jar ~/
```

**Linux:**
```bash
wget https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar -P ~/
```

### 5. Create a GCP project and service account
1. Go to [console.cloud.google.com](https://console.cloud.google.com) and create a new project under 'No Organization' (toggle Projects → Select a resource → No Organization)
2. Enable billing on the project — required to use GCS, BigQuery, and GCE. **GCP offers a $300 free credit** for new accounts which covers this project entirely.
3. Go to **IAM → Service Accounts → Create Service Account**
4. Add the following roles: BigQuery Admin, Storage Admin, Compute Admin, Service Account Admin, Project IAM Admin
5. Go to **Keys → Add Key → JSON** and download the key file
6. Place the key file at the project root and rename it `gcp-key.json`:
```
DE_ZoomCamp_FinalProject/gcp-key.json
```
> **Note:** Do NOT commit `gcp-key.json` to git — it is in `.gitignore` for this reason.
>
> **Note:** Do NOT create a GCS bucket or BigQuery dataset manually — these will be created automatically in Step 7 via Terraform.

### 6. Authenticate with GCP
```bash
gcloud auth login
gcloud auth application-default login
gcloud config set project YOUR_PROJECT_ID
```
> Replace `YOUR_PROJECT_ID` with your GCP project ID from Step 5 (e.g. `my-nba-project-123`).

### 7. Provision infrastructure (Terraform)
```bash
cd terraform/
terraform init
terraform apply
cd ..
```

Terraform will prompt you for three values:
- `project_id` — your GCP project ID from Step 5
- `bucket_name` — a globally unique name for your GCS bucket (e.g. `nba-pipeline-yourname-2025`)
- `region` — the GCP region closest to you (e.g. `us-east4`, `us-central1`, `europe-west1`)

This creates the GCS bucket, BigQuery dataset, Airflow VM, and service account with IAM roles. Note the bucket name and region you enter — you will need both in the next steps.

### 8. Set environment variables
> **Note:** `.env` is not committed to the repo for security reasons. Create your own by copying the example file and filling in the values from your GCP project and Step 7 Terraform output.

```bash
cp .env.example .env
# Open .env and fill in:
# GCS_BUCKET=your-gcs-bucket-name        ← bucket name you entered in Step 7
# GCP_PROJECT_ID=your-gcp-project-id     ← your GCP project ID from Step 5
# BIGQUERY_DATASET=nba_analytics          ← leave as is
```

Then load the variables:
```bash
export $(grep -v '^#' .env | xargs)
```

Verify:
```bash
echo $GCS_BUCKET
echo $GCP_PROJECT_ID
```

### 9. Create BigQuery tables
```bash
bq query --use_legacy_sql=false < BigQuerySQL/dashboard_views.sql
```

### 10. Ingest raw data locally
> **Important:** NBA.com actively blocks requests from GCP, AWS, and all major cloud providers. Ingestion must be run from your local machine.

```bash
python3 scripts/ingest_local.py
```
This fetches full 2024-25 season game and player data for 10 NBA stars and uploads raw JSON directly to GCS.

### 11. Start Airflow on the VM
> **Note:** Replace `us-east4-a` with the region you entered in Step 7 followed by `-a` (e.g. if you entered `us-central1`, use `us-central1-a`).

First copy your service account key to the VM:
```bash
gcloud compute scp gcp-key.json airflow-vm:~/gcp-key.json --zone=us-east4-a
```

Then SSH in, clone the repo, move the key, and start Airflow:
```bash
gcloud compute ssh airflow-vm --zone=us-east4-a
git clone https://github.com/JackBThompson/DE_ZoomCamp_FinalProject.git
cd DE_ZoomCamp_FinalProject
mv ~/gcp-key.json .
cp .env.example .env
# Fill in .env with your GCP values, then load them:
export $(grep -v '^#' .env | xargs)
docker-compose -f docker/docker-compose.yml up -d
```

Airflow UI is available at `http://localhost:8080` after startup.

### 12. Run the Spark transformation
Back on your local machine, export your environment variables and run Spark:
```bash
export $(grep -v '^#' .env | xargs)

spark-submit \
  --packages com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1 \
  --jars ~/gcs-connector-hadoop3-latest.jar \
  --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
  --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
  --conf spark.hadoop.google.cloud.auth.service.account.enable=true \
  --conf spark.hadoop.google.cloud.auth.service.account.json.keyfile=$(pwd)/gcp-key.json \
  spark/transform.py 2025-04-13
```

### 13. Verify data landed in BigQuery
```bash
bq query --use_legacy_sql=false 'SELECT COUNT(*) FROM `nba_analytics.game_stats`'
bq query --use_legacy_sql=false 'SELECT COUNT(*) FROM `nba_analytics.player_stats`'
```
---

## Why Tables Are Partitioned and Clustered

Both `game_stats` and `player_stats` are partitioned by `game_date` and clustered by `team_abbreviation` / `player_id`.

- **Partitioning** — dashboard queries always filter by date range. Partitioning means BigQuery only scans the relevant days rather than the entire table, reducing query cost by up to 90%.
- **Clustering** — most filters are team or player specific. Clustering lets BigQuery skip non-matching blocks entirely at no extra cost.

---

## Dashboard

View the live Looker Studio dashboard:
https://lookerstudio.google.com/u/0/reporting/787022c9-e531-44ce-9291-e9183fe5bf2e/page/dRZtF

Built on two tiles:
- **Top Scorers by Average Points** — categorical bar chart ranking 10 NBA stars by avg points per game across the 2024-25 season
- **Player Performance Over Time** — time series showing average points per player per month across the 2024-25 season

To recreate the dashboard, follow `lookerDashboard/looker_setup.md`.
To view screenshots, refer to `lookerDashboard/screenshots`
---

## Known Limitations

- **NBA.com blocks cloud IPs** — ingestion must run locally via `scripts/ingest_local.py`. This is a documented limitation of `nba_api` affecting GCP, AWS, and Azure since 2020.
- **Rate limiting** — `sleep(1)` between API calls is required to avoid NBA.com rate limits.
- **Unofficial API** — `nba_api` wraps undocumented NBA.com endpoints. Data availability depends on NBA.com uptime.
