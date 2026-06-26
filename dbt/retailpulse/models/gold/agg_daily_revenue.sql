SELECT
    order_date,
    primary_category,
    COUNT(DISTINCT order_id)                                                   AS total_orders,
    COUNT(DISTINCT user_id)                                                    AS unique_customers,
    COUNT(DISTINCT CASE WHEN is_first_order THEN user_id END)                 AS new_customers,
    SUM(total_amount)                                                          AS gross_revenue,
    SUM(total_amount) FILTER (WHERE status = 'delivered')                     AS net_revenue,
    AVG(total_amount)                                                          AS avg_order_value,
    ROUND(
        COUNT(*) FILTER (WHERE status = 'cancelled') * 100.0
        / NULLIF(COUNT(*), 0), 2
    )                                                                          AS cancellation_rate_pct,
    CURRENT_TIMESTAMP                                                          AS dbt_updated_at
FROM {{ ref('fct_orders') }}
GROUP BY order_date, primary_category
ORDER BY order_date DESC
