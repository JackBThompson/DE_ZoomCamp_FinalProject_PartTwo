-- Staging model: customers
-- Light cleaning only — normalize email, company name, cast types
-- Source: crm_analytics.raw_customers (written by Spark)

SELECT
    customer_id,
    name,
    LOWER(TRIM(email))      AS email,
    INITCAP(TRIM(company))  AS company,
    signup_date,
    LOWER(plan_tier)        AS plan_tier,
    country,
    ingestion_date
FROM {{ source('crm_analytics', 'raw_customers') }}
WHERE customer_id IS NOT NULL
  AND email IS NOT NULL
