-- Verify that order_001 in silver holds the latest updated CDC status ('cdc_demo_updated')
SELECT order_id, order_status
FROM {{ ref('silver_orders') }}
WHERE order_id = 'order_001' AND order_status != 'cdc_demo_updated'
