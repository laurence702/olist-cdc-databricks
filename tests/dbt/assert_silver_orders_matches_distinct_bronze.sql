-- Ensure silver order count matches distinct order_id count in bronze
WITH bronze_distinct AS (
    SELECT count(DISTINCT order_id) AS distinct_keys
    FROM {{ source('bronze', 'bronze_public_orders') }}
),
silver_count AS (
    SELECT count(*) AS total_rows
    FROM {{ ref('silver_orders') }}
)
SELECT *
FROM bronze_distinct, silver_count
WHERE distinct_keys != total_rows
