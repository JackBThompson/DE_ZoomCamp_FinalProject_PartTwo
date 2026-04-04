-- Staging model: deals
-- Cast and normalize — close_date is null for open deals (expected)

SELECT
    deal_id,
    customer_id,
    ROUND(deal_value, 2)    AS deal_value,
    LOWER(deal_stage)       AS deal_stage,
    close_date,
    owner,
    ingestion_date
FROM {{ source('crm_analytics', 'raw_deals') }}
WHERE deal_id IS NOT NULL
  AND customer_id IS NOT NULL
  AND deal_value > 0
