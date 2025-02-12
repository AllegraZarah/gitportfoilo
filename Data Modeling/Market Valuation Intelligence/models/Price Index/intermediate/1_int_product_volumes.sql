{{ config (materialized = 'ephemeral') }}

with
pre_ex_trans_vol as(
	-- Not all products make up the indeex, so we pull in for the concerned products only
	
	select trade_date
			, product
			, product_code
			, total_weight_kg

	from {{ ref ('stg_fact_marketflows') }}
	where product in ('Rice'
						, 'Palm'
						, 'Cotton'
						, 'Barley'
						, 'Tea'
						, 'Tumeric'
						, 'Peanuts'
						, 'Coffee')
			
			and trade_date is not null -- To ensure only completed trades are utilized
			and order_type = 'Purchase'
			and system = 'BASE'
			and asset_type in ('Futures', 'Options')
	),

ex_trans_vol as(
	-- Goal for this intermediate model is to derive the volume per product traded per year

	select extract('year' from trade_date) execution_year
			, concat((extract('year' from trade_date)), '-12-31') :: date year_end_date
			, concat((extract('year' from trade_date)) + 1, '-10-01') :: date effective_date 
				-- For each year, the volume from the previous year prior is used in computing the index
				-- Index computation starts from Oct 1, which corresponds roughly to the start of the new harvest year (season) and was chosen as a suitable start time
			, product
			, product_code
			, sum(total_weight_kg/1000) total_volume_mt
	
	from pre_ex_trans_vol
		
	group by extract('year' from trade_date), product, product_code
	order by product, extract('year' from trade_date)
	),

running_avg_ex_vol as(
	select *
			, avg(total_volume_mt) over (partition by product order by effective_date rows between 4 preceding and current row) AS five_yr_moving_avg_ex_vol_mt
				-- a 5-year moving average volume value is used in place of the actual volume transacted in the prior year to compute the current index
	
	from ex_trans_vol
	),

date_and_com as(
	select dim_date.date_actual date_
			, comm.product product_
			, comm.product_code product_code_
	
	from {{ ref ('stg_date') }} dim_date
	cross join (select distinct product, product_code from running_avg_ex_vol) comm
	
	where dim_date.date_actual between '2024-10-01' and current_date
	),

price_and_vol as (
	-- The goal here is to derive the weights of each product based on the nationally traded volume and volume traded internally
	-- Join the volumes and prices and get proper product code labels.
	select dac.date_
		, dac.product_
		, dac.product_code_
		, price.product_endofday_price endofday_price_product_kg
		, running_avg_ex_vol.*
		, nat_vol.volume nat_vol
		, nat_vol.five_yr_moving_avg_nat_vol_mt

	from date_and_com dac
	left join {{ ref ('stg_endofday_product_prices') }} price
		on date(dac.date_) = date(price.date)
		and dac.product_code_ = price.product_code
	left join {{ ref ('stg_national_vol') }} nat_vol
		on date(dac.date_) = date(nat_vol.effective_date)
		and dac.product_code_ = nat_vol.product_code
	left join running_avg_ex_vol
		on date(dac.date_) = date(running_avg_ex_vol.effective_date)
		and dac.product_code_ = running_avg_ex_vol.product_code
	),
	
adj_vol as (
	select case when extract(month from date_) >= 10 
					then concat(extract(year from date_)::text, '/', extract(year from date_ + INTERVAL '1 year')::text)
				else concat(extract(year from date_ - INTERVAL '1 year')::text, '/', extract(year from date_)::text)
				end season
			, date_ date
			, product_ product
			, product_code_ product_code
			, endofday_price_product_kg
			, endofday_price_product_kg * 1000 endofday_price_product_mt
			, coalesce(total_volume_mt, 0) ex_executed_vol_mt
			, coalesce(five_yr_moving_avg_ex_vol_mt, 0) five_yr_moving_avg_ex_vol_mt
			, coalesce(nat_vol_mt, 0) nat_vol_mt
			, coalesce(five_yr_moving_avg_nat_vol_mt, 0) five_yr_moving_avg_nat_vol_mt
	
	from price_and_vol
	)

select *
from adj_vol