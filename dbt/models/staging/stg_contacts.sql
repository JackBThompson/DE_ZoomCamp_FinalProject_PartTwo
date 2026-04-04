-- Staging model: contacts
-- Drop contacts with no matching customer (mismatched IDs already filtered in Spark)

SELECT
    contact_id,
    customer_id,
    first_name,
    last_name,
    LOWER(TRIM(email)) AS email,
    phone,
    role,
    ingestion_date
FROM {{ source('crm_analytics', 'raw_contacts') }}
WHERE contact_id IS NOT NULL
  AND customer_id IS NOT NULL
