## Objective: Scheduled DAG that uploads CRM CSV files to GCS and triggers the Spark transformation.

import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'raw')

SOURCE_FILES = [
    'customers.csv',
    'contacts.csv',
    'deals.csv',
    'activities.csv',
    'subscriptions.csv',
]

dag = DAG(
    dag_id='crm_ingestion',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
)


def upload_to_gcs(**context):
    execution_date = context['ds']
    bucket = os.environ.get('GCS_BUCKET')
    hook = GCSHook()

    for filename in SOURCE_FILES:
        local_path = os.path.join(DATA_DIR, filename)

        if not os.path.exists(local_path):
            raise FileNotFoundError(f"{local_path} not found — run scripts/generate_fake_data.py first")

        gcs_path = f'raw/crm/{execution_date}/{filename}'

        with open(local_path, 'r') as f:
            data = f.read()

        hook.upload(
            bucket_name=bucket,
            object_name=gcs_path,
            data=data,
            mime_type='text/csv',
        )
        print(f"Uploaded {filename} → gs://{bucket}/{gcs_path}")


upload_task = PythonOperator(
    task_id='upload_crm_files_to_gcs',
    python_callable=upload_to_gcs,
    dag=dag,
)

trigger_spark = BashOperator(
    task_id='trigger_spark',
    bash_command='spark-submit \
      --packages com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1 \
      --jars ~/gcs-connector-hadoop3-latest.jar \
      --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
      --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
      --conf spark.hadoop.google.cloud.auth.service.account.enable=true \
      --conf spark.hadoop.google.cloud.auth.service.account.json.keyfile={{ var.value.gcp_keyfile_path }} \
      ~/DE_ZoomCamp_FinalProject_PartTwo/spark/transform.py {{ ds }}',
    dag=dag,
)

upload_task >> trigger_spark
