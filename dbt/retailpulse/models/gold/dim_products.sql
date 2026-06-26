WITH base AS (
    SELECT * FROM {{ ref('stg_products') }}
),
sales AS (
    SELECT
        primary_category,
        COUNT(*)         AS order_count,
        SUM(total_amount) AS total_revenue
    FROM {{ ref('stg_transactions') }}
    WHERE status = 'delivered'
    GROUP BY primary_category
),
revenue_rank AS (
    SELECT
        primary_category,
        total_revenue,
        SUM(total_revenue) OVER ()                                        AS grand_total,
        SUM(total_revenue) OVER (ORDER BY total_revenue DESC
                                  ROWS BETWEEN UNBOUNDED PRECEDING
                                  AND CURRENT ROW)                        AS running_total
    FROM sales
)
SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.subcategory,
    p.brand,
    p.base_price,
    p.cost_price,
    p.gross_margin,
    p.margin_pct,
    p.supplier_id,
    CASE
        WHEN rr.running_total / NULLIF(rr.grand_total, 0) <= 0.80 THEN 'A'
        WHEN rr.running_total / NULLIF(rr.grand_total, 0) <= 0.95 THEN 'B'
        ELSE 'C'
    END                             AS abc_class,
    COALESCE(s.order_count, 0)      AS total_orders,
    COALESCE(s.total_revenue, 0)    AS total_revenue,
    CURRENT_TIMESTAMP               AS dbt_updated_at
FROM base p
LEFT JOIN sales s       ON p.category = s.primary_category
LEFT JOIN revenue_rank rr ON p.category = rr.primary_category
