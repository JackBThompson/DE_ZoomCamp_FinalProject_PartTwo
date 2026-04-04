-- Core model: dim_subscriptions
-- One row per subscription — partitioned by ingestion_date, clustered by plan and status

{{ config(
    materialized='table',
    partition_by={
        "field": "ingestion_date",
        "data_type": "date"
    },
    cluster_by=["plan", "status"]
) }}

SELECT
    s.subscription_id,
    s.customer_id,
    s.plan,
    s.mrr,
    s.renewal_date,
    s.status,
    c.company,
    c.country,
    c.signup_date                                               AS customer_since,
    DATE_DIFF(s.renewal_date, CURRENT_DATE(), DAY)              AS days_until_renewal,
    s.ingestion_date
FROM {{ ref('stg_subscriptions') }} s
LEFT JOIN {{ ref('stg_customers') }} c
    ON s.customer_id = c.customer_id
