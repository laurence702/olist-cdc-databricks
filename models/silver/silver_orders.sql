{{ config(
    materialized='table',
    file_format='delta'
) }}

-- Silver Orders: Deduplicates bronze CDC change log to the single latest row
-- per primary key (order_id), ordered by updated_at descending.
WITH ranked AS (
    SELECT
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date,
        updated_at,
        _cdc_record_hash,
        _cdc_ingested_at,
        row_number() OVER (
            PARTITION BY order_id
            ORDER BY updated_at DESC, _cdc_ingested_at DESC
        ) AS _row_num
    FROM {{ source('bronze', 'bronze_public_orders') }}
)

SELECT
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    updated_at,
    _cdc_record_hash,
    _cdc_ingested_at
FROM ranked
WHERE _row_num = 1
