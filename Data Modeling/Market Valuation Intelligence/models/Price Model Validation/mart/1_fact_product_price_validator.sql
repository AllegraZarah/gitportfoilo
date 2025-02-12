{{ config (materialized = 'table') }}

-- This query calculates adjusted closing prices for products across different markets
-- It handles missing prices and applies weight adjustments based on trading activity

with recursive
-- Get initial prices for the earliest date in the dataset
start_price as (
    select date, product_code, product, final_price_per_kg
    from {{ source ('public', 'fact_product_market_prices') }}
    where date = (select min(date_actual) from {{ref ('int_mean_market_prices')}})
    ),

-- Combine all prices with starting prices, filling gaps where needed
all_prices_with_start_price as (
    select all_prices.*, start_price.final_price_per_kg,
            -- Use starting price if market price is missing
            case when all_prices.market_price is null and start_price.final_price_per_kg is not null then start_price.final_price_per_kg
            else all_prices.final_market_price
            end adj_final_market_price
    from {{ref ('int_mean_market_prices')}} all_prices
    left join start_price
        on all_prices.date_actual = start_price.date
            and all_prices.product_code = start_price.product_code
    ),

-- Recursive CTE to calculate closing prices and handle missing data
closing_price as (
    -- Base case: Initial day's calculations
    select date_actual, 
            market_code, 
            product_code, 
            fixed_market_weight,
            fixed_market_weight adjusted_market_weight,
            final_market_price,
            adj_final_market_price,
            -- Track consecutive days without trading
            case when final_market_price is not null then 0 
                else 1 
            end as days_without_trade,
            
            1.00 daily_weight,
            adj_final_market_price * fixed_market_weight testing,
            -- Calculate weighted average price for product
            SUM(adj_final_market_price * fixed_market_weight) over (partition by product_code, date_actual order by date_actual) as product_acp_fixed_weight
    
    from all_prices_with_start_price
    
    where date_actual = (select min(date_actual) from all_prices_with_start_price)
    
    UNION ALL
    
    -- Recursive case: Calculate prices for subsequent days
    SELECT main.date_actual, 
            main.market_code, 
            main.product_code, 
            main.fixed_market_weight, 
            -- Adjust market weight to 0 if no trading for more than 5 days
            CASE WHEN main.final_market_price IS NULL AND cp.days_without_trade + 1 > 5 THEN 0
            ELSE main.fixed_market_weight
            END adjusted_market_weight,
            main.final_market_price, 
            -- Determine final price based on trading activity
            CASE WHEN main.final_market_price IS NOT NULL THEN main.final_market_price
                WHEN main.final_market_price IS NULL AND cp.days_without_trade + 1 <= 5 THEN cp.product_acp_fixed_weight
                WHEN main.final_market_price IS NULL AND cp.days_without_trade + 1 > 5 THEN 0
            END AS adj_final_market_price,
        
            -- Update days without trading counter
            CASE WHEN main.final_market_price IS NOT NULL THEN 0 
                ELSE cp.days_without_trade + 1 
            END AS days_without_trade,
        
            -- Calculate daily weight based on trading activity
            SUM(CASE WHEN main.final_market_price IS NULL AND cp.days_without_trade + 1 > 5 THEN 0
                    ELSE main.fixed_market_weight
                END) OVER (PARTITION BY cp.product_code, cp.date_actual ORDER BY cp.date_actual) daily_weight,

            -- Calculate test value for verification
            (CASE WHEN main.final_market_price IS NOT NULL THEN main.final_market_price
                    WHEN main.final_market_price IS NULL AND cp.days_without_trade + 1 <= 5 THEN cp.product_acp_fixed_weight
                    WHEN main.final_market_price IS NULL AND cp.days_without_trade + 1 > 5 THEN 0
                END) * 
                    (CASE WHEN main.final_market_price IS NULL AND cp.days_without_trade + 1 > 5 THEN 0
                        ELSE main.fixed_market_weight
                    END) testing,
            
            -- Calculate weighted average price with safeguard against division by zero
            SUM(
                (CASE WHEN main.final_market_price IS NOT NULL THEN main.final_market_price
                    WHEN main.final_market_price IS NULL AND cp.days_without_trade + 1 <= 5 THEN cp.product_acp_fixed_weight
                    WHEN main.final_market_price IS NULL AND cp.days_without_trade + 1 > 5 THEN 0
                END)
                * 
                (CASE WHEN main.final_market_price IS NULL AND cp.days_without_trade + 1 > 5 THEN 0
                    ELSE main.fixed_market_weight
                END)
                ) OVER (PARTITION BY cp.product_code, cp.date_actual ORDER BY cp.date_actual) 
            /
            CASE WHEN SUM(
                        CASE WHEN main.final_market_price IS NULL AND cp.days_without_trade + 1 > 5 THEN 0
                        ELSE main.fixed_market_weight
                        END
                        ) OVER (PARTITION BY cp.product_code, cp.date_actual ORDER BY cp.date_actual) = 0 
                        THEN 1 -- Prevent division by zero
                        ELSE SUM(
                                CASE WHEN main.final_market_price IS NULL AND cp.days_without_trade + 1 > 5 THEN 0
                                ELSE main.fixed_market_weight
                                END) OVER (PARTITION BY cp.product_code, cp.date_actual ORDER BY cp.date_actual)
            END AS product_acp_fixed_weight

    from all_prices_with_start_price main
    join closing_price cp
        on main.market_code = cp.market_code AND main.date_actual = cp.date_actual + INTERVAL '1 day'

    where main.date_actual <= (select max(date_actual) from all_prices_with_start_price)
    ),

-- Combine final prices with product information at market level
final_price_at_market_level AS (
    SELECT
        cp.date_actual, 
        f.consolidated_product_name, 
        cp.product_code, 
        cp.market_code, 
        cp.fixed_market_weight,
        f.final_market_price, 
        cp.adj_final_market_price, 
        cp.days_without_trade,
        cp.testing,
        cp.product_acp_fixed_weight
    FROM 
        closing_price cp
    JOIN 
        all_prices_with_start_price f ON cp.date_actual = f.date_actual AND cp.market_code = f.market_code
    ),

-- Get final closing prices at product level
product_closing_price as (
    select distinct date_actual, product_code, consolidated_product_name, product_acp_fixed_weight 
    
    from final_price_at_market_level
    )

-- Return final product-level closing prices
select *
from product_closing_price