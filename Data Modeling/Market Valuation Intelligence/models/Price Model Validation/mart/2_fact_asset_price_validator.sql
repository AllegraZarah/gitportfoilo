{{ config (materialized = 'table') }}

with 
assets_with_location as (
	select distinct asset_code, 
			substring(asset_code, 2) as product_code,
			case when asset_type = 'Options' then 'n/a'
			else location
			end location,
			CASE WHEN asset_code LIKE '%COF%' THEN 'Coffee'
					WHEN asset_code LIKE '%TEA%'  THEN 'Tea'
					WHEN asset_code LIKE '%TRM%' THEN 'Turmeric'
					WHEN asset_code LIKE '%RIC%' THEN 'Rice'
					WHEN asset_code LIKE '%PLM%' THEN 'Palm Oil'
					WHEN asset_code LIKE '%PNT%' THEN 'Peanuts'
					WHEN asset_code LIKE '%SGM%' THEN 'Sorghum'
					WHEN asset_code LIKE '%BRL%' THEN 'Barley'
					WHEN asset_code LIKE '%CRN%' THEN 'Corn'
			ELSE asset_code
			END consolidated_product_name
	
	from {{ source ('public', 'historical_asset_prices')}}
		
	where asset_type in ('Futures', 'Forward', 'Options') and (asset_code like '%COF%' or asset_code like '%TEA%' or 
							asset_code like '%TRM%' or asset_code like '%RIC%' or asset_code like '%PLM%' or 
							asset_code like '%PNT%' or  asset_code like '%SGM%' or asset_code like '%BRL%' or asset_code like '%CRN%')
	),
	
asset_closing_price as (
	select asset_date.date_actual, asset_with_loc.*, loc.state, 
			product_price.product_acp_fixed_weight,
			coalesce(trans_matrix.cost_adjustment_per_kg, 0) logistics_differential,
			(product_price.product_acp_fixed_weight - coalesce(trans_matrix.cost_adjustment_per_kg, 0)) asset_acp

	from {{ ref ('stg_date') }} asset_date
	cross join assets_with_location asset_with_loc
	left join {{ ref ('fact_product_price_validator')}} product_price
		on asset_with_loc.product_code = product_price.product_code
		and asset_date.date_actual = product_price.date_actual

	left join {{ source (public, 'geo_location') }} loc
		on asset_with_loc.location = loc.name
		
	left join {{ ref ('stg_transport_matrix') }} trans_matrix
		on asset_with_loc.consolidated_product_name = trans_matrix.product_label
		and loc.state = trans_matrix.destination_region
    )

select *
from asset_closing_price