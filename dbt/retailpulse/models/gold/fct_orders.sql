SELECT
    t.order_id,
    t.user_id,
    t.order_timestamp,
    t.order_date,
    DATE_TRUNC('week',  t.order_timestamp)  AS order_week,
    DATE_TRUNC('month', t.order_timestamp)  AS order_month,
    t.total_amount,
    t.item_count,
    t.primary_category,
    t.payment_method,
    t.status,
    t.country,
    t.city,
    c.tier                                   AS customer_tier,
    c.is_churned,
    CASE
        WHEN c.first_order_date::date = t.order_date THEN TRUE
        ELSE FALSE
    END                                      AS is_first_order,
    CURRENT_TIMESTAMP                        AS dbt_updated_at
FROM {{ ref('stg_transactions') }} t
LEFT JOIN {{ ref('dim_customers') }} c ON t.user_id = c.user_id
