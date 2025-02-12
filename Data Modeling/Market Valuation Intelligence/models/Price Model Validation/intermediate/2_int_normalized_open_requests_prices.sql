{{ config (materialized = 'ephemeral') }}

-- This intermediate model processes open trade orders, normalizing prices and filtering out outliers.
-- It ensures consistency in price adjustments and prepares the data for further aggregation or analysis.

with 
exchange_open_order as (
	-- Extract unique open trade orders, adjusting timestamps for consistency.
	select distinct record_id, trade_timestamp,
			CASE WHEN ((trade_timestamp at time zone 'WAT') :: time) >= '15:00:00'
				THEN DATE((trade_timestamp at time zone 'WAT') + INTERVAL '1 day')
			ELSE DATE((trade_timestamp at time zone 'WAT'))
			END AS adjusted_trade_time, -- This is necessary because any transaction from the 3pm mark on each day is recorded as a next-day trade
			transaction_type,
			requested_price requested_price_per_unit, requested_units,
			requested_price/weight_per_unit requested_price_per_kg, requested_units*weight_per_unit requested_volume_kg, asset_type, asset_code, 
			product_code, product, grouped_product_name,
			asset_location, seller_region_code, buyer_region_code, 
			case when buyer_region_code is null then asset_location else buyer_region_code end destination_location -- Determines the final destination of the product
	
	from {{ ref ('stg_fact_marketflows') }}
	where transaction_id is null -- Filters out completed transactions
	),

normalized_exchange_open_order_price as (
	-- Adjust order prices based on transportation cost differentials.
	select price.*, log_.cost_adjustment_per_kg, 
			(price.requested_price_per_kg + coalesce(log_.cost_adjustment_per_kg,0)) AS adjusted_order_price, 
			price.requested_volume_kg * (price.requested_price_per_kg + coalesce(log_.cost_adjustment_per_kg,0)) AS adjusted_order_value
			
	from exchange_open_order price
	left join {{ ref ('stg_transport_matrix') }} log_
	on price.grouped_product_name = log_.product_label
		and price.destination_location = log_.destination_region
	),	
	
avg_std_exchange_open_order as (
	-- Compute the average and standard deviation for normalized order prices per asset per day.
	select *, 
			avg(adjusted_order_price) over (partition by transaction_type, asset_code, adjusted_trade_time order by adjusted_trade_time) average_order_price_per_kg,
			coalesce(stddev(adjusted_order_price) over (partition by transaction_type, asset_code, adjusted_trade_time order by adjusted_trade_time) , 0) stddev_order_price_per_kg,
			(avg(adjusted_order_price) over (partition by transaction_type, asset_code, adjusted_trade_time order by adjusted_trade_time)  - coalesce(stddev(adjusted_order_price) over (partition by transaction_type, asset_code, adjusted_trade_time order by adjusted_trade_time) , 0)) lower_band,
			(avg(adjusted_order_price) over (partition by transaction_type, asset_code, adjusted_trade_time order by adjusted_trade_time)  + coalesce(stddev(adjusted_order_price) over (partition by transaction_type, asset_code, adjusted_trade_time order by adjusted_trade_time) , 0)) upper_band
	
	from normalized_exchange_open_order_price
	),

without_outlier_unionized_open_trans as (
	-- Remove outliers that fall outside of the calculated price bands.
	select *
	from avg_std_exchange_open_order
	where adjusted_order_price between lower_band and upper_band
	)

-- Final selection of the cleaned and normalized open trade orders.
select *
from without_outlier_unionized_open_trans