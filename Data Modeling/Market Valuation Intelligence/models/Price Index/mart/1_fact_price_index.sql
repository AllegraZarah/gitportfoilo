{{ config (
	materialized = 'table',
	post_hook = [
		"GRANT USAGE ON SCHEMA analytics_mart TO etl_user;"
		"GRANT SELECT ON analytics_mart.fact_price_index TO etl_user"
	] 
	) }}

with

index_price as(	
	
	select *
		, case when dow = 0 then -- Saturday
					lead(woy, 1) over (partition by product order by date)
				when dow = 6 then -- Sunday
					lead(woy, 2) over (partition by product order by date)
			else woy
			end adj_woy -- Prior weekend trades are taken as new week trades, kind of an extension of monday trades
		
		, case when endofday_pdt_price_mt is null or endofday_pdt_price_mt = 0 then null
				else product_weight_5yr_avg
			end product_weight
			
		, sum(coalesce(endofday_pdt_price_mt, 0) * 
				case when endofday_pdt_price_mt is null or endofday_pdt_price_mt = 0 then null
				else product_weight_5yr_avg
				end) over (partition by date order by date) 
			/
				sum(case when endofday_pdt_price_mt is null or endofday_pdt_price_mt = 0 then null
					else product_weight_5yr_avg
					end) over (partition by date order by date) index_price
		-- The above line is to ensure the weighted average is by only products with prices on the concerned day

	from {{ ref ('int_product_weights') }}
	),

all_index as (
	select season, date, product, product_code, product_weight, closing_price_product_kg price_kg, endofday_pdt_price_mt price_mt
			, null initial_sub_index, index_price, null initial_index, adj_woy, quarter
			, case when date between '2024-10-01' and current_date then 'First Base Period'
				else null
				end index_period_identifier 
				-- First Base period starts Oct 1, 2024. 
				-- This identifier is necesary in preparation for new base periods which woud only require an update to the case statement

	from index_price
	),

new_index as ( 
	select *
			, ((index_price / first_value(index_price) over (partition by index_period_identifier order by date)) * 100) index
			, ((price_mt / first_value(price_mt) over (partition by index_period_identifier, product order by date)) * 100) sub_index -- These are indexes of individual products that mkae up the main index

			, concat(adj_woy, '_', case when extract(dow from date::timestamp) = 6 then 0 
										when extract(dow from date::timestamp) = 0 then 1
										when extract(dow from date::timestamp) = 1 then 2
										when extract(dow from date::timestamp) = 2 then 3
										when extract(dow from date::timestamp) = 3 then 4
										when extract(dow from date::timestamp) = 4 then 5
										when extract(dow from date::timestamp) = 5 then 6 
									end) wow_identifier-- Week starts on Sat & ends on Fri and by default dow is labeled from Mon - Sun (1 - 0)
			, concat(extract(month from date::timestamp), '_', extract(year from date::timestamp)) month_identifier-- Week starts on Sat & ends on Fri and by default dow is labeled from Mon - Sun (1 - 0)

	from all_index
		),

change_values as (
	-- Computing the changes in the index and sub index per period
	select *
			, (lag(index, 1) over (partition by product order by date)) previous_day_index
			, (lag(sub_index, 1) over (partition by product order by date)) previous_day_sub_index
			
			, (lag(index, 7) over (partition by product order by date)) previous_week_index
			, (lag(sub_index, 7) over (partition by product order by date)) previous_week_sub_index

			, first_value(index) over (partition by product, adj_woy order by date) week_start_index
			, first_value(index) over (partition by product, adj_woy order by date desc) week_end_index
			, first_value(sub_index) over (partition by product, adj_woy order by date) week_start_sub_index
			, first_value(sub_index) over (partition by product, adj_woy order by date desc) week_end_sub_index

			, first_value(index) over (partition by product, month_identifier order by date) month_start_index
			, first_value(index) over (partition by product, month_identifier order by date desc) month_end_index
			, first_value(sub_index) over (partition by product, month_identifier order by date) month_start_sub_index
			, first_value(sub_index) over (partition by product, month_identifier order by date desc) month_end_sub_index

			, first_value(index) over (partition by product, quarter order by date) quarter_start_index
			, first_value(index) over (partition by product, quarter order by date desc) quarter_end_index
			, first_value(sub_index) over (partition by product, quarter order by date) quarter_start_sub_index
			, first_value(sub_index) over (partition by product, quarter order by date desc) quarter_end_sub_index
	
			, (first_value(index) over (partition by season, product order by date)) season_start_index
			, (first_value(sub_index) over (partition by season, product order by date)) season_start_sub_index
			, (first_value(index) over (partition by extract('year' from date), product order by date)) year_start_index
			, (first_value(sub_index) over (partition by extract('year' from date), product order by date)) year_start_sub_index
	
	from new_index
	),

last_period_values as (
	select *

			, lag(month_end_index) over (partition by product order by date) last_month_end_index
			, lag(month_end_sub_index) over (partition by product order by date) last_month_end_sub_index
	
			, lag(quarter_end_index) over (partition by product order by date) last_quarter_end_index
			, lag(quarter_end_sub_index) over (partition by product order by date) last_quarter_end_sub_index

	from change_values
	
	),

previous_period_value as (
	select *

			, first_value(last_month_end_index) over (partition by product, month_identifier order by date) previous_month_end_index 
			, first_value(last_month_end_sub_index) over (partition by product, month_identifier order by date) previous_month_end_sub_index 
	
			, first_value(last_quarter_end_index) over (partition by product, quarter order by date) previous_quarter_end_index 
			, first_value(last_quarter_end_sub_index) over (partition by product, quarter order by date) previous_quarter_end_sub_index 

	from last_period_values
	),
	
changes as (
	select *
			, ((index / previous_day_index) - 1 ) dod_index_change
			, ((sub_index / previous_day_sub_index) - 1 ) dod_sub_index_change
	
			, ((index / previous_week_index) - 1 ) wow_index_change
			, ((sub_index / previous_week_sub_index) - 1 ) wow_sub_index_change

			, ((index / month_start_index) - 1) mtd_index_change
			, ((sub_index / month_start_sub_index) - 1) mtd_sub_index_change
	
			, ((month_end_index / previous_month_end_index) - 1) mom_index_change
			, ((month_end_sub_index / previous_month_end_sub_index) - 1) mom_sub_index_change

			, ((index / quarter_start_index) - 1) qtd_index_change
			, ((sub_index / quarter_start_sub_index) - 1) qtd_sub_index_change
	
			, ((quarter_end_index / previous_quarter_end_index) - 1) qoq_index_change
			, ((quarter_end_sub_index / previous_quarter_end_sub_index) - 1) qoq_sub_index_change
	
			, ((index / season_start_index) - 1 ) std_index_change
			, ((sub_index / season_start_sub_index) - 1 ) std_sub_index_change
	
			, ((index / year_start_index) - 1 ) ytd_index_change
			, ((sub_index / year_start_sub_index) - 1 ) ytd_sub_index_change
	
	from previous_period_value
	),
	
final_index as (

	select
		distinct date
			, 'Index' product_code
			, 'Index' product
			, null :: double precision product_weight
			, index_price endofday_index_price_mt
			, index points
			, previous_day_index prev_day_point
			, dod_index_change dod_change
			, previous_week_index prev_week_point
			, wow_index_change wow_change
			, week_start_index week_start
			, week_end_index week_end
			, month_start_index month_start
			, month_end_index month_end
			, mtd_index_change mtd_change
			, previous_month_end_index previous_month_end
			, mom_index_change mom_change
			, quarter_start_index quarter_start
			, quarter_end_index quarter_end
			, qtd_index_change qtd_change
			, previous_quarter_end_index previous_quarter_end
			, qoq_index_change qoq_change
			, season_start_index season_start
			, std_index_change std_change
			, year_start_index year_start
			, ytd_index_change ytd_change
	
	from changes
	
	UNION
	
	select date
			, product_code
			, product
			, product_weight
			, price_mt endofday_index_price_mt
			, sub_index points
			, previous_day_sub_index prev_day_point
			, dod_sub_index_change dod_change
			, previous_week_sub_index prev_week_point
			, wow_sub_index_change wow_change
			, week_start_sub_index week_start
			, week_end_sub_index week_end
			, month_start_sub_index month_start
			, month_start_sub_index month_end
			, mtd_sub_index_change mtd_change
			, previous_month_end_sub_index previous_month_end
			, mom_sub_index_change mom_change
			, quarter_start_sub_index quarter_start
			, quarter_end_sub_index quarter_end
			, qtd_sub_index_change qtd_change
			, previous_quarter_end_sub_index previous_quarter_end
			, qoq_sub_index_change qoq_change
			, season_start_sub_index season_start
			, std_sub_index_change std_change
			, year_start_sub_index year_start
			, ytd_sub_index_change ytd_change


	from changes
	)

select *

from final_index