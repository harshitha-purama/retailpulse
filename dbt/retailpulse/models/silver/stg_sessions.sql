SELECT
    session_id,
    user_id,
    start_time,
    end_time,
    duration_seconds,
    page_views,
    products_viewed,
    add_to_cart_count,
    converted,
    device_type,
    country,
    DATE(start_time) AS session_date,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM {{ source('silver', 'sessions') }}
WHERE start_time IS NOT NULL
  AND session_id IS NOT NULL
