-- Objective: Creates raw tables and analytics views in BigQuery for the CRM migration pipeline.
-- Tables are partitioned by ingestion_date and clustered for cost-efficient querying.
-- Views power the Looker Studio dashboard tiles.
-- Run: bq query --use_legacy_sql=false < bigQuerySQL/dashboard_views.sql


-- ── Raw Tables (written by Spark) ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS crm_analytics.raw_customers (
    customer_id    INT64,
    name           STRING,
    email          STRING,
    company        STRING,
    signup_date    DATE,
    plan_tier      STRING,
    country        STRING,
    ingestion_date DATE
)
PARTITION BY ingestion_date
CLUSTER BY plan_tier;


CREATE TABLE IF NOT EXISTS crm_analytics.raw_contacts (
    contact_id     INT64,
    customer_id    INT64,
    first_name     STRING,
    last_name      STRING,
    email          STRING,
    phone          STRING,
    role           STRING,
    ingestion_date DATE
)
PARTITION BY ingestion_date
CLUSTER BY customer_id;


CREATE TABLE IF NOT EXISTS crm_analytics.raw_deals (
    deal_id        INT64,
    customer_id    INT64,
    deal_value     FLOAT64,
    deal_stage     STRING,
    close_date     DATE,
    owner          STRING,
    ingestion_date DATE
)
PARTITION BY ingestion_date
CLUSTER BY deal_stage;


CREATE TABLE IF NOT EXISTS crm_analytics.raw_activities (
    activity_id    INT64,
    customer_id    INT64,
    activity_type  STRING,
    timestamp      TIMESTAMP,
    outcome        STRING,
    notes          STRING,
    ingestion_date DATE
)
PARTITION BY ingestion_date
CLUSTER BY activity_type;


CREATE TABLE IF NOT EXISTS crm_analytics.raw_subscriptions (
    subscription_id INT64,
    customer_id     INT64,
    plan            STRING,
    mrr             FLOAT64,
    renewal_date    DATE,
    status          STRING,
    ingestion_date  DATE
)
PARTITION BY ingestion_date
CLUSTER BY plan, status;


-- ── Dashboard View 1: Deal Distribution by Stage (categorical tile) ───────────
-- Bar chart — deal count and total value per stage

CREATE OR REPLACE VIEW crm_analytics.deal_distribution_by_stage AS
SELECT
    deal_stage,
    COUNT(*)                   AS deal_count,
    ROUND(SUM(deal_value), 2)  AS total_value,
    ROUND(AVG(deal_value), 2)  AS avg_value
FROM crm_analytics.raw_deals
GROUP BY deal_stage
ORDER BY deal_count DESC;


-- ── Dashboard View 2: Customer Signups Over Time (temporal tile) ──────────────
-- Line chart — new customers per month broken down by plan tier

CREATE OR REPLACE VIEW crm_analytics.customer_signups_over_time AS
SELECT
    DATE_TRUNC(signup_date, MONTH) AS signup_month,
    plan_tier,
    COUNT(*)                       AS new_customers
FROM crm_analytics.raw_customers
WHERE signup_date IS NOT NULL
GROUP BY signup_month, plan_tier
ORDER BY signup_month;


-- ── Supporting View: MRR by Plan ──────────────────────────────────────────────

CREATE OR REPLACE VIEW crm_analytics.mrr_by_plan AS
SELECT
    plan,
    status,
    COUNT(*)             AS customer_count,
    ROUND(SUM(mrr), 2)  AS total_mrr,
    ROUND(AVG(mrr), 2)  AS avg_mrr
FROM crm_analytics.raw_subscriptions
WHERE mrr IS NOT NULL
GROUP BY plan, status
ORDER BY total_mrr DESC;


-- ── Supporting View: Pipeline Health Check ────────────────────────────────────

CREATE OR REPLACE VIEW crm_analytics.pipeline_health_check AS
SELECT
    ingestion_date,
    COUNT(*) AS records_ingested
FROM crm_analytics.raw_customers
WHERE ingestion_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY ingestion_date
ORDER BY ingestion_date DESC;
