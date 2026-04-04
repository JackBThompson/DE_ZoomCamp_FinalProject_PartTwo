-- Core model: dim_customers
-- One row per customer — enriched with subscription and activity summary
-- Partitioned by ingestion_date, clustered by plan_tier

{{ config(
    materialized='table',
    partition_by={
        "field": "ingestion_date",
        "data_type": "date"
    },
    cluster_by=["plan_tier"]
) }}

SELECT
    c.customer_id,
    c.name,
    c.email,
    c.company,
    c.signup_date,
    c.plan_tier,
    c.country,
    s.mrr,
    s.status                            AS subscription_status,
    s.renewal_date,
    COUNT(DISTINCT a.activity_id)       AS total_activities,
    MAX(a.timestamp)                    AS last_activity_at,
    c.ingestion_date
FROM {{ ref('stg_customers') }} c
LEFT JOIN {{ ref('stg_subscriptions') }} s
    ON c.customer_id = s.customer_id
LEFT JOIN {{ ref('stg_activities') }} a
    ON c.customer_id = a.customer_id
GROUP BY
    c.customer_id, c.name, c.email, c.company, c.signup_date,
    c.plan_tier, c.country, s.mrr, s.status, s.renewal_date, c.ingestion_date
