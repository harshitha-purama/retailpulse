WITH first_orders AS (
    SELECT
        user_id,
        DATE_TRUNC('month', MIN(order_timestamp)) AS cohort_month
    FROM {{ ref('fct_orders') }}
    GROUP BY user_id
),
orders_with_cohort AS (
    SELECT
        f.user_id,
        f.cohort_month,
        DATE_TRUNC('month', o.order_timestamp)                                       AS order_month,
        EXTRACT(
            EPOCH FROM DATE_TRUNC('month', o.order_timestamp) - f.cohort_month
        ) / (86400.0 * 30)                                                           AS period_number
    FROM first_orders f
    JOIN {{ ref('fct_orders') }} o ON f.user_id = o.user_id
),
cohort_counts AS (
    SELECT
        cohort_month,
        period_number,
        COUNT(DISTINCT user_id) AS active_users
    FROM orders_with_cohort
    WHERE period_number BETWEEN 0 AND 11
    GROUP BY cohort_month, period_number
)
SELECT
    cohort_month,
    period_number,
    active_users,
    FIRST_VALUE(active_users) OVER (
        PARTITION BY cohort_month ORDER BY period_number
    )                                                                                AS cohort_size,
    ROUND(
        active_users * 100.0
        / NULLIF(FIRST_VALUE(active_users) OVER (
            PARTITION BY cohort_month ORDER BY period_number
        ), 0),
        2
    )                                                                                AS retention_rate,
    CURRENT_TIMESTAMP                                                                AS dbt_updated_at
FROM cohort_counts
ORDER BY cohort_month, period_number
