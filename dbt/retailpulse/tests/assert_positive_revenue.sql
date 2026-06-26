-- Fail if any order has negative total amount
SELECT order_id
FROM {{ ref('fct_orders') }}
WHERE total_amount < 0
