{{ config(materialized = 'table') }}

with
pre_changes_values as (
    select 
        nnm.*,
        case 
            when nnm.dow = 0 then lead(nnm.woy, 1) over (partition by nnm.product order by nnm.date)
            when nnm.dow = 6 then lead(nnm.woy, 2) over (partition by nnm.product order by nnm.date)
            else nnm.woy
        end as adj_woy,
        case 
            when extract('year' from date) < 2022 and extract('month' from date) >= 12 then 
                concat(extract('year' from date)::text, '/', extract('year' from date + interval '1 year')::text)
            when extract('year' from date) >= 2022 and extract('month' from date) >= 10 then 
                concat(extract('year' from date)::text, '/', extract('year' from date + interval '1 year')::text)
            else 
                concat(extract('year' from date - interval '1 year')::text, '/', extract('year' from date)::text)
        end as season,
        concat('Q', extract(quarter from date::timestamp), ' ', extract(year from date::timestamp)) as quarter,
        concat(extract(month from date::timestamp), '_', extract(year from date::timestamp)) as month_identifier
    from {{ ref('int_prices_commodities_prices') }} nnm
),

changes_values as (
    select 
        *,
        lag(eod_price, 1) over (partition by product order by date) as previous_day_price,
        lag(eod_price, 7) over (partition by product order by date) as previous_week_price,
        lag(eod_price, 1) over (partition by product, day_of_month order by date) as previous_month_price,
        lag(eod_price, 1) over (partition by product, day_of_quarter order by date) as previous_quarter_price,
        first_value(eod_price) over (partition by product, adj_woy order by date) as week_start_price,
        first_value(eod_price) over (partition by product, adj_woy order by date desc) as week_end_price,
        first_value(eod_price) over (partition by product, month_identifier order by date) as month_start_price,
        first_value(eod_price) over (partition by product, month_identifier order by date desc) as month_end_price,
        first_value(eod_price) over (partition by product, quarter order by date) as quarter_start_price,
        first_value(eod_price) over (partition by product, quarter order by date desc) as quarter_end_price,
        first_value(eod_price) over (partition by season, product order by date) as season_start_price_,
        first_value(eod_price) over (partition by extract('year' from date), product order by date) as year_start_price_
    from pre_changes_values
),

adjusted_changes_for_year_season_start_prices as (
    select 
        *,
        case 
            when season_start_price_ is null and eod_price is not null then 
                first_value(eod_price) over (partition by season, product order by (case when eod_price is not null then date end))
            else season_start_price_
        end as season_start_price,
        case 
            when year_start_price_ is null and eod_price is not null then 
                first_value(eod_price) over (partition by extract('year' from date), product order by (case when eod_price is not null then date end))
            else year_start_price_
        end as year_start_price
    from changes_values
),

last_period_values as (
    select 
        *,
        lag(month_end_price) over (partition by product order by date) as last_month_end_price,
        lag(quarter_end_price) over (partition by product order by date) as last_quarter_end_price
    from adjusted_changes_for_year_season_start_prices
),

previous_period_value as (
    select 
        *,
        first_value(last_month_end_price) over (partition by product, month_identifier order by date) as previous_month_end_price,
        first_value(last_quarter_end_price) over (partition by product, quarter order by date) as previous_quarter_end_price
    from last_period_values
),

changes as (
    select 
        *,
        (eod_price - sod_price) as dod_price_diff,
        ((eod_price / nullif(sod_price, 0)) - 1) as dod_price_change,
        ((eod_price / nullif(week_start_price, 0)) - 1) as wtd_price_change,
        ((eod_price / nullif(previous_week_price, 0)) - 1) as wow_price_change,
        ((eod_price / nullif(month_start_price, 0)) - 1) as mtd_price_change,
        ((eod_price / nullif(previous_month_price, 0)) - 1) as mom_price_change,
        ((month_end_price / nullif(previous_month_end_price, 0)) - 1) as month_end_price_change,
        ((eod_price / nullif(quarter_start_price, 0)) - 1) as qtd_price_change,
        ((eod_price / nullif(previous_quarter_price, 0)) - 1) as qoq_price_change,
        ((quarter_end_price / nullif(previous_quarter_end_price, 0)) - 1) as quarter_end_price_change,
        ((eod_price / nullif(season_start_price, 0)) - 1) as std_price_change,
        ((eod_price / nullif(year_start_price, 0)) - 1) as ytd_price_change
    from previous_period_value
),

final as (
    select 
        date, dow, adj_woy as woy, season, quarter,
        product_code, product,
        eod_price as eod_price_kg, sod_price as sod_price_kg,
        max_price_kg, min_price_kg,
        dod_price_diff, dod_price_change,
        week_start_price, week_end_price, wtd_price_change, previous_week_price, wow_price_change,
        month_start_price, month_end_price, mtd_price
