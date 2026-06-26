SELECT
    order_id,
    user_id,
    timestamp AS order_timestamp,
    DATE(timestamp) AS order_date,
    total_amount,
    item_count,
    primary_category,
    payment_method,
    status,
    country,
    city,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM {{ source('silver', 'transactions') }}
WHERE status IS NOT NULL
  AND order_id IS NOT NULL
