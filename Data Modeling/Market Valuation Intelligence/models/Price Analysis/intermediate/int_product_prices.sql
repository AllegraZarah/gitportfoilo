{{ config(materialized = 'ephemeral') }}

with
closing_sod_price as (
    select 
        date,
        extract(dow from price.date::timestamp) as dow,
        price.day_of_month,
        price.day_of_quarter,
        price.day_of_year,
        concat(extract(year from price.date::timestamp), '_', extract(week from price.date::timestamp)) as woy,
        price.product_code,
        price.product_name as product,
        price.product_eod_price as eod_price,
        lag(price.product_eod_price, 1) over (partition by price.product_code order by price.date) as sod_price
    
    from {{ ref('stg_product_prices') }} price
    where date >= '2020-01-01'
),	

trade_transactions as (
    select *
    from {{ ref('stg_fact_marketflows') }}
),
	
high_low_prices as ( -- Determine the highest and lowest price per product traded on a given day
    select 
        date(deal_created_at) as date,
        product,
        max(trade_price / calc_volume_per_unit) as max_price_kg,
        min(trade_price / calc_volume_per_unit) as min_price_kg
    from trade_transactions
    where order_type = 'Purchase' 
        and (source_name = 'SOURCE1' or source_name is null or source_name = '')
    group by date(deal_created_at), product
    order by date(deal_created_at), product
),	

products_prices as (
    select 
        closing_sod_price.*,
        high_low_prices.max_price_kg,
        high_low_prices.min_price_kg
    from closing_sod_price
    left join high_low_prices
        on closing_sod_price.date = high_low_prices.date
        and closing_sod_price.product = high_low_prices.product
),

non_null_minmax as (
    select 
        date, dow, woy, day_of_month, day_of_quarter, day_of_year,
        product_code, product, 
        eod_price, sod_price,
        case 
            when (max_price_kg is null) and (eod_price >= sod_price) then eod_price
            when (max_price_kg is null) and (eod_price < sod_price) then sod_price
            else max_price_kg
        end as max_price_kg,
        case 
            when (min_price_kg is null) and (eod_price > sod_price) then sod_price
            when (min_price_kg is null) and (eod_price <= sod_price) then eod_price
            else min_price_kg
        end as min_price_kg
    from products_prices
)

select *
from non_null_minmax
