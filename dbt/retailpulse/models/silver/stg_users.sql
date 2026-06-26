SELECT
    id AS user_id,
    email,
    name,
    signup_date,
    country,
    tier,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM {{ source('public', 'users') }}
