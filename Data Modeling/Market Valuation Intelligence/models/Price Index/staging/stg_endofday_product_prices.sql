{{ config (materialized = 'ephemeral') }}

WITH end_of_day_product_prices AS (
    SELECT * 
    FROM {{ source ('analytics_mart', 'fact_product_market_prices') }}
)

SELECT * 
FROM end_of_day_product_prices