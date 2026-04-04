-- Staging model: subscriptions
-- MRR is null for free tier — that is expected, not a data quality issue

SELECT
    subscription_id,
    customer_id,
    LOWER(plan)     AS plan,
    mrr,
    renewal_date,
    LOWER(status)   AS status,
    ingestion_date
FROM {{ source('crm_analytics', 'raw_subscriptions') }}
WHERE subscription_id IS NOT NULL
  AND customer_id IS NOT NULL
