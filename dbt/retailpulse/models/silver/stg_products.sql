SELECT
    id AS product_id,
    name AS product_name,
    category,
    subcategory,
    brand,
    base_price,
    cost_price,
    base_price - cost_price AS gross_margin,
    ROUND((base_price - cost_price) / NULLIF(base_price, 0) * 100, 2) AS margin_pct,
    supplier_id,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM {{ source('public', 'products') }}
