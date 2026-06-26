SELECT
    session_date,
    COUNT(DISTINCT session_id)                                                          AS total_sessions,
    COUNT(DISTINCT CASE WHEN page_views > 0       THEN session_id END)                 AS page_view_sessions,
    COUNT(DISTINCT CASE WHEN products_viewed > 0  THEN session_id END)                 AS product_view_sessions,
    COUNT(DISTINCT CASE WHEN add_to_cart_count > 0 THEN session_id END)                AS add_to_cart_sessions,
    COUNT(DISTINCT CASE WHEN converted            THEN session_id END)                 AS converted_sessions,
    ROUND(
        COUNT(DISTINCT CASE WHEN products_viewed > 0 THEN session_id END) * 100.0
        / NULLIF(COUNT(DISTINCT session_id), 0), 2
    )                                                                                   AS product_view_rate,
    ROUND(
        COUNT(DISTINCT CASE WHEN add_to_cart_count > 0 THEN session_id END) * 100.0
        / NULLIF(COUNT(DISTINCT CASE WHEN products_viewed > 0 THEN session_id END), 0), 2
    )                                                                                   AS cart_rate,
    ROUND(
        COUNT(DISTINCT CASE WHEN converted THEN session_id END) * 100.0
        / NULLIF(COUNT(DISTINCT CASE WHEN add_to_cart_count > 0 THEN session_id END), 0), 2
    )                                                                                   AS checkout_conversion_rate,
    ROUND(
        COUNT(DISTINCT CASE WHEN converted THEN session_id END) * 100.0
        / NULLIF(COUNT(DISTINCT session_id), 0), 2
    )                                                                                   AS overall_conversion_rate,
    CURRENT_TIMESTAMP                                                                   AS dbt_updated_at
FROM {{ ref('stg_sessions') }}
GROUP BY session_date
ORDER BY session_date DESC
