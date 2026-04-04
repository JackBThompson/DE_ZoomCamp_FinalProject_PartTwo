-- Staging model: activities
-- Filter out rows where timestamp failed to parse (landed as NULL)

SELECT
    activity_id,
    customer_id,
    LOWER(activity_type)    AS activity_type,
    timestamp,
    LOWER(outcome)          AS outcome,
    notes,
    ingestion_date
FROM {{ source('crm_analytics', 'raw_activities') }}
WHERE activity_id IS NOT NULL
  AND customer_id IS NOT NULL
  AND timestamp IS NOT NULL
