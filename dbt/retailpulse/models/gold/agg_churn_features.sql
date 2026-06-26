SELECT
    c.user_id,
    c.days_since_last_order,
    c.total_orders,
    c.total_revenue,
    c.avg_order_value,
    ROUND(
        c.cancelled_orders * 100.0 / NULLIF(c.total_orders, 0), 2
    )                                                        AS cancellation_rate,
    r.r_score,
    r.f_score,
    r.m_score,
    r.rfm_total,
    r.rfm_segment,
    COALESCE(s.avg_session_duration, 0)                      AS avg_session_duration,
    COALESCE(s.avg_products_viewed, 0)                       AS avg_products_viewed,
    COALESCE(s.total_sessions_30d, 0)                        AS total_sessions_30d,
    c.is_churned                                             AS label,
    CURRENT_TIMESTAMP                                        AS dbt_updated_at
FROM {{ ref('dim_customers') }} c
LEFT JOIN {{ ref('agg_rfm_scores') }} r ON c.user_id = r.user_id
LEFT JOIN (
    SELECT
        user_id,
        AVG(duration_seconds)                                AS avg_session_duration,
        AVG(products_viewed)                                 AS avg_products_viewed,
        COUNT(*) FILTER (
            WHERE session_date >= CURRENT_DATE - INTERVAL '30 days'
        )                                                    AS total_sessions_30d
    FROM {{ ref('stg_sessions') }}
    GROUP BY user_id
) s ON c.user_id = s.user_id
WHERE c.total_orders > 0
