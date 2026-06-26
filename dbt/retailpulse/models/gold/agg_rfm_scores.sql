WITH rfm_raw AS (
    SELECT
        user_id,
        MAX(order_date)                                                         AS last_order_date,
        EXTRACT(EPOCH FROM (CURRENT_DATE - MAX(order_date))) / 86400           AS recency_days,
        COUNT(DISTINCT order_id)                                                AS frequency,
        SUM(total_amount) FILTER (WHERE status = 'delivered')                  AS monetary
    FROM {{ ref('fct_orders') }}
    GROUP BY user_id
),
rfm_scored AS (
    SELECT
        user_id,
        last_order_date,
        recency_days,
        frequency,
        monetary,
        NTILE(5) OVER (ORDER BY recency_days DESC) AS r_score,
        NTILE(5) OVER (ORDER BY frequency)          AS f_score,
        NTILE(5) OVER (ORDER BY monetary)            AS m_score
    FROM rfm_raw
)
SELECT
    user_id,
    last_order_date,
    ROUND(recency_days, 0)  AS recency_days,
    frequency,
    ROUND(COALESCE(monetary, 0), 2) AS monetary,
    r_score,
    f_score,
    m_score,
    (r_score + f_score + m_score)   AS rfm_total,
    CASE
        WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
        WHEN r_score >= 3 AND f_score >= 3 AND m_score >= 3 THEN 'Loyal Customers'
        WHEN r_score >= 4 AND f_score <= 2               THEN 'Recent Customers'
        WHEN r_score >= 3 AND m_score >= 4               THEN 'Potential Loyalists'
        WHEN r_score <= 2 AND f_score >= 3 AND m_score >= 3 THEN 'At Risk'
        WHEN r_score <= 2 AND f_score >= 2               THEN 'Cannot Lose Them'
        WHEN r_score <= 2 AND f_score <= 2 AND m_score <= 2 THEN 'Lost'
        ELSE 'Hibernating'
    END                              AS rfm_segment,
    CURRENT_TIMESTAMP                AS dbt_updated_at
FROM rfm_scored
