{{ config(
    materialized='table',
    file_format='delta'
) }}

-- Silver Customers: Deduplicates bronze CDC records to the single latest row
-- per primary key (customer_id), ordered by updated_at descending.
WITH ranked AS (
    SELECT
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        updated_at,
        _cdc_record_hash,
        _cdc_ingested_at,
        row_number() OVER (
            PARTITION BY customer_id
            ORDER BY updated_at DESC, _cdc_ingested_at DESC
        ) AS _row_num
    FROM {{ source('bronze', 'bronze_public_customers') }}
)

SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    updated_at,
    _cdc_record_hash,
    _cdc_ingested_at
FROM ranked
WHERE _row_num = 1
