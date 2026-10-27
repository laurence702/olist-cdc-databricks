-- Ensure silver customer count matches distinct customer_id count in bronze
WITH bronze_distinct AS (
    SELECT count(DISTINCT customer_id) AS distinct_keys
    FROM {{ source('bronze', 'bronze_public_customers') }}
),
silver_count AS (
    SELECT count(*) AS total_rows
    FROM {{ ref('silver_customers') }}
)
SELECT *
FROM bronze_distinct, silver_count
WHERE distinct_keys != total_rows
