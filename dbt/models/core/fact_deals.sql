-- Core model: fact_deals
-- One row per deal — partitioned by ingestion_date, clustered by deal_stage and plan_tier
-- Joins to dim_customers to enrich with customer attributes

{{ config(
    materialized='table',
    partition_by={
        "field": "ingestion_date",
        "data_type": "date"
    },
    cluster_by=["deal_stage", "plan_tier"]
) }}

SELECT
    d.deal_id,
    d.customer_id,
    d.deal_value,
    d.deal_stage,
    d.close_date,
    d.owner,
    c.plan_tier,
    c.company,
    c.country,
    d.ingestion_date
FROM {{ ref('stg_deals') }} d
LEFT JOIN {{ ref('stg_customers') }} c
    ON d.customer_id = c.customer_id
