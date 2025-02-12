{{ config (materialized = 'ephemeral') }}

with 

national_vol as (
	select *
			, avg(volume) over (partition by commodity_code order by effective_date rows between 4 preceding and current row) AS five_yr_moving_avg_nat_vol_mt

	from {{ source ('analytics_mart', 'national_trade_volume') }}
            -- these volumes are the national volume from the previous period (that is 1 year prior).
            -- This was gotten from the market research team, with date sourced from FAOSTAT (The statistics arm of the Food and Agriculture Organization of the United Nations.)
	)

select *
from national_vol