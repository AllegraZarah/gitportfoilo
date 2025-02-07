{{ config (materialized = 'ephemeral') }}

-- This query calculates market prices and volumes from both executed and open orders
-- It combines historical transaction data with current open orders to provide a complete market view

with 
-- Calculate cumulative market values and volumes for each market code by date
market_summary_ex_matched_wb_trans as ( 
    select *, sum(adjusted_value) over (partition by market_code, record_date order by record_date) market_value
            , sum(volume_kg) over (partition by market_code, record_date order by record_date) market_volume
        
    from {{ ref ('int_normalized_executed_prices')}}
    ),

-- Summarize market data to get price per kg for each market
summarized_exchange_matched_wb_trans as (
    select distinct record_date, grouped_product_name, item_label, item_id, market_code, market_value, market_volume, (market_value / market_volume) market_price
    
    from market_summary_ex_matched_wb_trans
    ),

-- Prepare distinct combinations of products and markets for cross joining with dates
for_crossjoining as (
    select distinct grouped_product_name, item_label product, item_id product_code, market_code
    
    from summarized_exchange_matched_wb_trans
    ),

-- Create a complete date range for each market-product combination
distinct_day_executed_order as (
    select dim_date.date_actual, market_code.*, trans.market_value, trans.market_volume, trans.market_price

    from {{ ref ('stg_date') }} dim_date
    cross join (select distinct grouped_product_name, product, product_code, market_code from for_crossjoining) market_code
    left join summarized_exchange_matched_wb_trans trans
    on dim_date.date_actual = trans.record_date
        and market_code.market_code = trans.market_code

    where (dim_date.date_actual between '2024-10-01' and current_date) 
    
    ),

-- Calculate best (lowest) sell prices and total volume for each market
open_sell as (
    select adjusted_trade_time, transaction_type, product, product_code, asset_code, adjusted_order_price, requested_volume_kg,
            min(adjusted_order_price) over (partition by adjusted_trade_time, transaction_type, asset_code) best_sell,
            sum(requested_volume_kg) over (partition by adjusted_trade_time, transaction_type, asset_code) market_volume
    
    from {{ ref ('int_normalized_open_order_prices')}}

    where transaction_type = 'Vend'
    order by adjusted_trade_time, asset_code, transaction_type
),

-- Get distinct best sell prices and volumes
best_sell as (
    select distinct adjusted_trade_time, transaction_type, product, product_code, asset_code, best_sell, market_volume
    from open_sell
),

-- Calculate best (highest) buy prices and total volume for each market
open_buy as (
    select adjusted_trade_time, transaction_type, product, product_code, asset_code, adjusted_order_price, requested_volume_kg,
            max(adjusted_order_price) over (partition by adjusted_trade_time, transaction_type, asset_code) best_buy,
            sum(requested_volume_kg) over (partition by adjusted_trade_time, transaction_type, asset_code) market_volume
    
    from {{ ref ('int_normalized_open_order_prices')}}

    where transaction_type = 'Purchase'
    order by adjusted_trade_time, asset_code, transaction_type
),

-- Get distinct best buy prices and volumes
best_buy as (
    select distinct adjusted_trade_time, transaction_type, product, product_code, asset_code, best_buy, market_volume
    from open_buy    
),

-- Calculate midpoint prices from best buy and sell orders
-- Only includes records where both buy and sell orders exist
open_order_prices_midpoint as (
    select dim_date.date_actual, all_open.market_code, all_open.product_code, all_open.product, best_buy.best_buy, best_sell.best_sell,
            case when best_buy.best_buy is null or best_sell.best_sell is null then (coalesce(best_buy.best_buy,0) + coalesce(best_sell.best_sell,0)) / 1
                    else (best_buy.best_buy + best_sell.best_sell) / 2
                    end mean_open_price,
            case when best_buy.market_volume is null or best_sell.market_volume is null then (coalesce(best_buy.market_volume,0) + coalesce(best_sell.market_volume,0))
                    else (best_buy.market_volume + best_sell.market_volume) / 2
                    end total_market_order_volume
    
    from {{ ref ('stg_date') }} dim_date
    cross join (select distinct grouped_product_name, product, product_code, market_code from for_crossjoining) all_open
    left join best_buy
    on dim_date.date_actual = best_buy.adjusted_trade_time
        and all_open.market_code = best_buy.asset_code
    left join best_sell
    on dim_date.date_actual = best_sell.adjusted_trade_time
        and all_open.market_code = best_sell.asset_code

    where (dim_date.date_actual between '2024-10-01' and current_date) 
            and best_buy.best_buy is not null
            and best_sell.best_sell is not null
    
    order by date_actual
    ),

-- Handle edge cases where prices or volumes are zero
adj_open_order_prices_midpoint as (
    select date_actual, market_code, product_code, product, best_buy, best_sell,
            case when mean_open_price = 0 then null
                    else mean_open_price
                    end mean_open_price,
            case when total_market_order_volume = 0 then null
                    else total_market_order_volume
                    end total_market_order_volume
    
    from open_order_prices_midpoint    
    ),

-- Final combination of executed and open order prices
-- Includes market weights based on market code prefixes
all_prices as (
    select ddeo.*, adj_average_open_order_prices.best_buy, adj_average_open_order_prices.best_sell, 
            adj_average_open_order_prices.mean_open_price, adj_average_open_order_prices.total_market_order_volume,
            case when ddeo.market_price is null then adj_average_open_order_prices.mean_open_price else ddeo.market_price end final_market_price,
            case when ddeo.market_volume is null then adj_average_open_order_prices.total_market_order_volume else ddeo.market_volume end final_market_volume,
            -- Assign fixed weights based on market type
            case when ddeo.market_code like 'Fo%' then 0.05  -- Forward markets
                when ddeo.market_code like 'Fu%' then 0.10   -- Futures markets
                when ddeo.market_code like 'Inv%' then 0.10  -- Inventory markets
                when ddeo.market_code like 'Op%' then 0.75   -- Options markets
            end as fixed_market_weight
    
    from distinct_day_executed_order ddeo
    left join adj_open_order_prices_midpoint
    on ddeo.date_actual = adj_open_order_prices_midpoint.date_actual
        and ddeo.market_code = adj_open_order_prices_midpoint.market_code
    )

-- Return all calculated prices, volumes, and weights
select *
from all_prices