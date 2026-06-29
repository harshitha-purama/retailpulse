-- ============================================================
-- SEED SILVER + GOLD LAYERS (fixed schema)
-- ============================================================
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- ── SILVER ──────────────────────────────────────────────────

DROP TABLE IF EXISTS silver.transactions CASCADE;
CREATE TABLE silver.transactions AS
WITH u AS (SELECT id FROM users ORDER BY random()),
     p AS (SELECT id, base_price FROM products ORDER BY random())
SELECT
    'TXN-' || LPAD(gs::TEXT, 6, '0')     AS transaction_id,
    (SELECT id FROM u OFFSET (gs % 500) LIMIT 1) AS user_id,
    (SELECT id FROM p OFFSET (gs % 200) LIMIT 1) AS product_id,
    (SELECT base_price FROM p OFFSET (gs % 200) LIMIT 1) AS unit_price,
    (random() * 4 + 1)::INT               AS quantity,
    CASE WHEN random() < 0.80 THEN 'completed'
         WHEN random() < 0.50 THEN 'cancelled'
         ELSE 'pending' END               AS status,
    CASE WHEN random() < 0.50 THEN 'credit_card'
         WHEN random() < 0.50 THEN 'paypal'
         ELSE 'debit_card' END            AS payment_method,
    NOW() - (random() * 365)::INT * INTERVAL '1 day'
           -(random() * 86400)::INT * INTERVAL '1 second' AS created_at
FROM generate_series(1, 5000) gs;

ALTER TABLE silver.transactions ADD COLUMN total_amount NUMERIC(10,2);
UPDATE silver.transactions SET total_amount = quantity * unit_price;

DROP TABLE IF EXISTS silver.sessions CASCADE;
CREATE TABLE silver.sessions AS
WITH u AS (SELECT id FROM users ORDER BY random())
SELECT
    'SES-' || LPAD(gs::TEXT, 7, '0')     AS session_id,
    (SELECT id FROM u OFFSET (gs % 500) LIMIT 1) AS user_id,
    (random() * 15 + 1)::INT              AS page_views,
    (random() * 8)::INT                   AS products_viewed,
    (random() * 4)::INT                   AS add_to_cart,
    CASE WHEN random() < 0.40 THEN 1 ELSE 0 END AS checkout_initiated,
    CASE WHEN random() < 0.25 THEN 1 ELSE 0 END AS purchased,
    CASE WHEN random() < 0.60 THEN 'mobile'
         WHEN random() < 0.75 THEN 'desktop'
         ELSE 'tablet' END               AS device_type,
    NOW() - (random() * 365)::INT * INTERVAL '1 day' AS session_start
FROM generate_series(1, 15000) gs;

-- ── GOLD ────────────────────────────────────────────────────

DROP TABLE IF EXISTS gold.dim_customers CASCADE;
CREATE TABLE gold.dim_customers AS
SELECT
    u.id                                           AS user_id,
    u.email,
    u.name,
    u.signup_date,
    u.country,
    COUNT(t.transaction_id)                        AS total_orders,
    COALESCE(SUM(t.total_amount), 0)               AS lifetime_value,
    MAX(t.created_at)                              AS last_order_date,
    CASE WHEN COALESCE(SUM(t.total_amount), 0) > 1000 THEN 'Platinum'
         WHEN COALESCE(SUM(t.total_amount), 0) > 400  THEN 'Gold'
         WHEN COALESCE(SUM(t.total_amount), 0) > 100  THEN 'Silver'
         ELSE 'Bronze' END                         AS tier,
    CASE WHEN MAX(t.created_at) < NOW() - INTERVAL '90 days'
         THEN TRUE ELSE FALSE END                  AS is_churned
FROM users u
LEFT JOIN silver.transactions t ON u.id = t.user_id AND t.status = 'completed'
GROUP BY u.id, u.email, u.name, u.signup_date, u.country;

DROP TABLE IF EXISTS gold.dim_products CASCADE;
CREATE TABLE gold.dim_products AS
SELECT
    p.id                                           AS product_id,
    p.name,
    p.category,
    p.subcategory,
    p.base_price,
    p.cost_price,
    COUNT(t.transaction_id)                        AS total_orders,
    COALESCE(SUM(t.total_amount), 0)               AS total_revenue,
    CASE WHEN COALESCE(SUM(t.total_amount), 0) >
         PERCENTILE_CONT(0.80) WITHIN GROUP (ORDER BY COALESCE(sub.rev,0))
              OVER () THEN 'A'
         WHEN COALESCE(SUM(t.total_amount), 0) >
         PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY COALESCE(sub.rev,0))
              OVER () THEN 'B'
         ELSE 'C' END                              AS abc_class
FROM products p
LEFT JOIN silver.transactions t ON p.id = t.product_id AND t.status = 'completed'
LEFT JOIN (SELECT product_id, SUM(total_amount) rev FROM silver.transactions
           WHERE status='completed' GROUP BY product_id) sub ON p.id = sub.product_id
GROUP BY p.id, p.name, p.category, p.subcategory, p.base_price, p.cost_price, sub.rev;

DROP TABLE IF EXISTS gold.fct_orders CASCADE;
CREATE TABLE gold.fct_orders AS
SELECT
    t.transaction_id, t.user_id, t.product_id,
    pr.category, pr.subcategory,
    t.quantity, t.unit_price, t.total_amount,
    t.status, t.payment_method, t.created_at,
    c.tier AS customer_tier
FROM silver.transactions t
JOIN gold.dim_customers c ON t.user_id  = c.user_id
JOIN gold.dim_products  p ON t.product_id = p.product_id
JOIN products pr         ON t.product_id = pr.id;

DROP TABLE IF EXISTS gold.agg_daily_revenue CASCADE;
CREATE TABLE gold.agg_daily_revenue AS
SELECT
    DATE_TRUNC('day', t.created_at)::DATE AS revenue_date,
    pr.category,
    COUNT(t.transaction_id)               AS total_orders,
    SUM(t.total_amount)                   AS total_revenue,
    AVG(t.total_amount)                   AS avg_order_value,
    COUNT(DISTINCT t.user_id)             AS unique_customers
FROM silver.transactions t
JOIN products pr ON t.product_id = pr.id
WHERE t.status = 'completed'
GROUP BY DATE_TRUNC('day', t.created_at)::DATE, pr.category;

DROP TABLE IF EXISTS gold.agg_rfm_scores CASCADE;
CREATE TABLE gold.agg_rfm_scores AS
WITH rfm_base AS (
    SELECT user_id,
           MAX(created_at)         AS last_purchase,
           COUNT(transaction_id)   AS frequency,
           SUM(total_amount)       AS monetary
    FROM silver.transactions WHERE status='completed' GROUP BY user_id
),
scored AS (
    SELECT user_id,
           EXTRACT(EPOCH FROM (NOW() - last_purchase))::INT/86400 AS recency_days,
           frequency, monetary,
           NTILE(5) OVER (ORDER BY last_purchase DESC) AS r_score,
           NTILE(5) OVER (ORDER BY frequency)          AS f_score,
           NTILE(5) OVER (ORDER BY monetary)           AS m_score
    FROM rfm_base
)
SELECT user_id, recency_days, frequency, monetary,
       r_score, f_score, m_score,
       r_score+f_score+m_score AS rfm_total,
       CASE WHEN r_score>=4 AND f_score>=4 THEN 'Champions'
            WHEN r_score>=3 AND f_score>=3 THEN 'Loyal Customers'
            WHEN r_score>=4 AND f_score<=2 THEN 'Recent Customers'
            WHEN r_score<=2 AND f_score>=3 THEN 'At Risk'
            WHEN r_score<=2 AND f_score<=2 THEN 'Lost'
            ELSE 'Potential Loyalists' END AS rfm_segment
FROM scored;

DROP TABLE IF EXISTS gold.agg_cohort_retention CASCADE;
CREATE TABLE gold.agg_cohort_retention AS
WITH first_order AS (
    SELECT user_id, DATE_TRUNC('month', MIN(created_at))::DATE AS cohort_month
    FROM silver.transactions WHERE status='completed' GROUP BY user_id
),
activity AS (
    SELECT DISTINCT user_id, DATE_TRUNC('month', created_at)::DATE AS active_month
    FROM silver.transactions WHERE status='completed'
)
SELECT
    f.cohort_month,
    (DATE_PART('year', a.active_month) - DATE_PART('year', f.cohort_month))*12 +
    (DATE_PART('month',a.active_month) - DATE_PART('month',f.cohort_month)) AS months_since_first,
    COUNT(DISTINCT f.user_id) AS retained_users,
    (SELECT COUNT(DISTINCT user_id) FROM first_order fo WHERE fo.cohort_month=f.cohort_month) AS cohort_size
FROM first_order f
JOIN activity a ON f.user_id=a.user_id AND a.active_month>=f.cohort_month
WHERE (DATE_PART('year', a.active_month)-DATE_PART('year', f.cohort_month))*12 +
      (DATE_PART('month',a.active_month)-DATE_PART('month',f.cohort_month)) <= 11
GROUP BY f.cohort_month,
    (DATE_PART('year', a.active_month)-DATE_PART('year', f.cohort_month))*12 +
    (DATE_PART('month',a.active_month)-DATE_PART('month',f.cohort_month));

DROP TABLE IF EXISTS gold.agg_funnel CASCADE;
CREATE TABLE gold.agg_funnel AS
SELECT
    DATE_TRUNC('day', session_start)::DATE AS funnel_date,
    COUNT(*)                               AS total_sessions,
    SUM(products_viewed)                   AS product_views,
    SUM(add_to_cart)                       AS add_to_carts,
    SUM(checkout_initiated)                AS checkouts,
    SUM(purchased)                         AS purchases,
    ROUND(SUM(purchased)::NUMERIC/NULLIF(COUNT(*),0)*100,2) AS conversion_rate
FROM silver.sessions
GROUP BY DATE_TRUNC('day', session_start)::DATE;

DROP TABLE IF EXISTS gold.agg_churn_features CASCADE;
CREATE TABLE gold.agg_churn_features AS
SELECT
    c.user_id, c.total_orders, c.lifetime_value,
    COALESCE(EXTRACT(EPOCH FROM (NOW()-c.last_order_date))::INT/86400, 999) AS days_since_last_order,
    COALESCE(r.recency_days, 999) AS recency_days,
    COALESCE(r.frequency, 0)      AS frequency,
    COALESCE(r.monetary,  0)      AS monetary,
    COALESCE(r.r_score,   1)      AS r_score,
    COALESCE(r.f_score,   1)      AS f_score,
    COALESCE(r.m_score,   1)      AS m_score,
    c.tier,
    c.is_churned::INT             AS label
FROM gold.dim_customers c
LEFT JOIN gold.agg_rfm_scores r ON c.user_id = r.user_id;

SELECT 'ALL DONE' AS status,
       (SELECT COUNT(*) FROM gold.dim_customers)    AS customers,
       (SELECT COUNT(*) FROM gold.agg_rfm_scores)   AS rfm_rows,
       (SELECT COUNT(*) FROM gold.agg_daily_revenue) AS daily_rev_rows,
       (SELECT COUNT(*) FROM gold.agg_funnel)        AS funnel_rows;
