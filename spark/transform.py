## Objective: Reads raw CRM CSVs from GCS, cleans and standardizes all 5 sources, writes Parquet to GCS and loads to BigQuery raw tables.

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

execution_date = sys.argv[1]

bucket  = os.environ.get('GCS_BUCKET')
project = os.environ.get('GCP_PROJECT_ID')
dataset = os.environ.get('BIGQUERY_DATASET')
keyfile = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', 'gcp-key.json')

spark = SparkSession.builder \
    .appName('crm_transform') \
    .config('spark.hadoop.fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem') \
    .config('spark.hadoop.fs.AbstractFileSystem.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS') \
    .config('spark.hadoop.google.cloud.auth.service.account.enable', 'true') \
    .config('spark.hadoop.google.cloud.auth.service.account.json.keyfile', keyfile) \
    .getOrCreate()

spark.conf.set('temporaryGcsBucket', bucket)

RAW_BASE      = f'gs://{bucket}/raw/crm/{execution_date}'
PROCESSED_BASE = f'gs://{bucket}/processed/crm/{execution_date}'

ingestion_date = F.to_date(F.lit(execution_date), 'yyyy-MM-dd')


# ── Shared helper ──────────────────────────────────────────────────────────────
# Normalizes both MM/DD/YYYY and YYYY-MM-DD into a DATE column

def parse_date_col(df, col_name):
    return df.withColumn(
        col_name,
        F.coalesce(
            F.to_date(F.col(col_name), 'yyyy-MM-dd'),
            F.to_date(F.col(col_name), 'MM/dd/yyyy'),
        )
    )

def parse_ts_col(df, col_name):
    return df.withColumn(
        col_name,
        F.coalesce(
            F.to_timestamp(F.col(col_name), 'yyyy-MM-dd HH:mm:ss'),
            F.to_timestamp(F.col(col_name), 'MM/dd/yyyy HH:mm'),
        )
    )


# ── customers ─────────────────────────────────────────────────────────────────

df_customers = spark.read.option('header', True).csv(f'{RAW_BASE}/customers.csv')

df_customers = df_customers \
    .withColumn('customer_id', F.col('customer_id').cast(T.IntegerType())) \
    .withColumn('email', F.lower(F.trim(F.col('email')))) \
    .withColumn('company', F.initcap(F.trim(F.col('company')))) \
    .withColumn('ingestion_date', ingestion_date)

df_customers = parse_date_col(df_customers, 'signup_date')

# Deduplicate on email — keep first occurrence
window = __import__('pyspark.sql.window', fromlist=['Window']).Window.partitionBy('email').orderBy('customer_id')
df_customers = df_customers \
    .withColumn('_row', F.row_number().over(window)) \
    .filter(F.col('_row') == 1) \
    .drop('_row')


# ── contacts ──────────────────────────────────────────────────────────────────

df_contacts = spark.read.option('header', True).csv(f'{RAW_BASE}/contacts.csv')

valid_customer_ids = df_customers.select('customer_id')

df_contacts = df_contacts \
    .withColumn('contact_id', F.col('contact_id').cast(T.IntegerType())) \
    .withColumn('customer_id', F.col('customer_id').cast(T.IntegerType())) \
    .withColumn('email', F.lower(F.trim(F.col('email')))) \
    .withColumn('ingestion_date', ingestion_date)

# Drop contacts with mismatched customer_ids
df_contacts = df_contacts.join(valid_customer_ids, on='customer_id', how='inner')


# ── deals ─────────────────────────────────────────────────────────────────────

df_deals = spark.read.option('header', True).csv(f'{RAW_BASE}/deals.csv')

df_deals = df_deals \
    .withColumn('deal_id', F.col('deal_id').cast(T.IntegerType())) \
    .withColumn('customer_id', F.col('customer_id').cast(T.IntegerType())) \
    .withColumn('deal_value', F.col('deal_value').cast(T.DoubleType())) \
    .withColumn('ingestion_date', ingestion_date)

df_deals = parse_date_col(df_deals, 'close_date')


# ── activities ────────────────────────────────────────────────────────────────

df_activities = spark.read.option('header', True).csv(f'{RAW_BASE}/activities.csv')

df_activities = df_activities \
    .withColumn('activity_id', F.col('activity_id').cast(T.IntegerType())) \
    .withColumn('customer_id', F.col('customer_id').cast(T.IntegerType())) \
    .withColumn('ingestion_date', ingestion_date)

df_activities = parse_ts_col(df_activities, 'timestamp')


# ── subscriptions ─────────────────────────────────────────────────────────────

df_subscriptions = spark.read.option('header', True).csv(f'{RAW_BASE}/subscriptions.csv')

df_subscriptions = df_subscriptions \
    .withColumn('subscription_id', F.col('subscription_id').cast(T.IntegerType())) \
    .withColumn('customer_id', F.col('customer_id').cast(T.IntegerType())) \
    .withColumn('mrr', F.col('mrr').cast(T.DoubleType())) \
    .withColumn('ingestion_date', ingestion_date)

df_subscriptions = parse_date_col(df_subscriptions, 'renewal_date')


# ── Write Parquet to GCS processed zone ───────────────────────────────────────

for name, df in [
    ('customers',     df_customers),
    ('contacts',      df_contacts),
    ('deals',         df_deals),
    ('activities',    df_activities),
    ('subscriptions', df_subscriptions),
]:
    df.write.mode('overwrite').parquet(f'{PROCESSED_BASE}/{name}/')
    print(f"Written {name} to {PROCESSED_BASE}/{name}/")


# ── Load to BigQuery raw dataset ───────────────────────────────────────────────

def write_bq(df, table, partition_field, cluster_fields):
    df.write.format('bigquery') \
        .option('table', f'{project}.{dataset}.{table}') \
        .option('partitionField', partition_field) \
        .option('clusteredFields', ','.join(cluster_fields)) \
        .option('writeMethod', 'direct') \
        .mode('append') \
        .save()
    print(f"Loaded {table} → BigQuery")


write_bq(df_customers,     'raw_customers',     'ingestion_date', ['plan_tier'])
write_bq(df_contacts,      'raw_contacts',      'ingestion_date', ['customer_id'])
write_bq(df_deals,         'raw_deals',         'ingestion_date', ['deal_stage'])
write_bq(df_activities,    'raw_activities',    'ingestion_date', ['activity_type'])
write_bq(df_subscriptions, 'raw_subscriptions', 'ingestion_date', ['plan', 'status'])

spark.stop()
