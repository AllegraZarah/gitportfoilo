{{ config (materialized = 'ephemeral') }}

with
seasonal_cumm_volumes as (
	-- We need to compute the cumulative volume for each year per product and for all product for the internal trades and national
	-- Although this is really a way of forward filling since the volume for the year is already known, but it was joined to a single date per year
	select *
		
			, sum(five_yr_moving_avg_ex_vol_mt) over (partition by season, product order by date) prior_seas_5yr_avg_ex_com_cumm_vol_mt
			, sum(five_yr_moving_avg_ex_vol_mt) over (partition by season order by date) prior_seas_5yr_avg_ex_cumm_vol_mt
	
			, sum(five_yr_moving_avg_nat_vol_mt) over (partition by season, product order by date) prior_seas_5yr_avg_nat_com_cumm_vol_mt
			, sum(five_yr_moving_avg_nat_vol_mt) over (partition by season order by date) prior_seas_5yr_avg_nat_cumm_vol_mt
	
	from {{ ref ('int_product_volumes') }}
    where product in ('Rice'
						, 'Palm'
						, 'Cotton'
						, 'Barley'
						, 'Tea'
						, 'Tumeric'
						, 'Peanuts'
						, 'Coffee')
	),
	
product_wt  as ( 
	-- Now we compute the weight for each product per year
	-- The national and internal transactions each make up 50:50 of the index
	select *
		, ((prior_seas_5yr_avg_ex_com_cumm_vol_mt * 0.5) / (nullif(prior_seas_5yr_avg_ex_cumm_vol_mt, 0))) exchange_weight_5yr_avg
		, ((prior_seas_5yr_avg_nat_com_cumm_vol_mt * 0.5) / (nullif(prior_seas_5yr_avg_nat_cumm_vol_mt, 0))) national_weight_5yr_avg
		, ((prior_seas_5yr_avg_ex_com_cumm_vol_mt * 0.5) / (nullif(prior_seas_5yr_avg_ex_cumm_vol_mt, 0))) + ((prior_seas_5yr_avg_nat_com_cumm_vol_mt * 0.5) / (nullif(prior_seas_5yr_avg_nat_cumm_vol_mt, 0))) product_weight_5yr_avg
	
		, extract(dow from date::timestamp) dow
		, concat(extract(year from date::timestamp), '_', extract(week from date::timestamp)) woy
		, concat('Q', extract(quarter from date::timestamp), ' ', extract(year from date::timestamp)) quarter
		
	from seasonal_cumm_volumes
	)

select *
from product_wt