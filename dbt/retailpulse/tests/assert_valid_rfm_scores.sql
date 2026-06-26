-- Fail if any RFM score is outside the 1-5 range (NTILE(5) guarantees this, but guard anyway)
SELECT user_id
FROM {{ ref('agg_rfm_scores') }}
WHERE r_score NOT BETWEEN 1 AND 5
   OR f_score NOT BETWEEN 1 AND 5
   OR m_score NOT BETWEEN 1 AND 5
