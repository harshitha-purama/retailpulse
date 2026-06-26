WITH base AS (
    SELECT * FROM {{ ref('stg_users') }}
),
orders AS (
    SELECT
        user_id,
        COUNT(*)                                                   AS total_orders,
        SUM(total_amount)                                          AS total_revenue,
        AVG(total_amount)                                          AS avg_order_value,
        MIN(order_timestamp)                                       AS first_order_date,
        MAX(order_timestamp)                                       AS last_order_date,
        COUNT(*) FILTER (WHERE status = 'delivered')              AS completed_orders,
        COUNT(*) FILTER (WHERE status = 'cancelled')              AS cancelled_orders
    FROM {{ ref('stg_transactions') }}
    GROUP BY user_id
),
metrics AS (
    SELECT
        user_id,
        total_revenue,
        total_orders,
        avg_order_value,
        first_order_date,
        last_order_date,
        completed_orders,
        cancelled_orders,
        EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_order_date)) / 86400 AS days_since_last_order,
        ROUND(
            total_revenue
            / NULLIF(EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - first_order_date)) / 86400, 0)
            * 365,
            2
        ) AS annual_revenue_rate
    FROM orders
)
SELECT
    b.user_id,
    b.email,
    b.name,
    b.country,
    b.tier,
    b.signup_date,
    COALESCE(m.total_orders, 0)                             AS total_orders,
    COALESCE(m.total_revenue, 0)                            AS total_revenue,
    COALESCE(m.avg_order_value, 0)                          AS avg_order_value,
    m.first_order_date,
    m.last_order_date,
    COALESCE(m.completed_orders, 0)                         AS completed_orders,
    COALESCE(m.cancelled_orders, 0)                         AS cancelled_orders,
    COALESCE(m.days_since_last_order, 9999)                 AS days_since_last_order,
    COALESCE(m.annual_revenue_rate, 0)                      AS annual_revenue_rate,
    COALESCE(m.annual_revenue_rate * 3, 0)                  AS predicted_clv_3yr,
    CASE WHEN m.days_since_last_order > 90 THEN TRUE
         ELSE FALSE END                                      AS is_churned,
    CURRENT_TIMESTAMP                                        AS dbt_updated_at
FROM base b
LEFT JOIN metrics m ON b.user_id = m.user_id
