{{ config(materialized = 'ephemeral') }}

with
price as (
    -- Retrieve the commodities' closing prices
    -- The upper part of the union includes closing prices from an external source not in the database.
    -- Data before a specific date is sourced externally, while on-system data is used thereafter.
    
    select date(date) as date,
           product_code,
           product_name,
           (price * 10) / 1000 as product_eod_price,
           'historical_source' as price_source
    from {{ source('public', 'historical_product_prices') }}

    UNION

    select price.date,
           comm.product_code,
           comm.product_name,
           price.price as product_eod_price,
           'current_source' as price_source
    from {{ source('public', 'product_prices') }} price
    left join {{ source('public', 'product_metadata') }} comm
    on price.product_code = comm.code
    where price.price != 0
),

ranked_prices as (
    select *,
           row_number() over (partition by date, product_code order by date, price_source) as row_num
    from price
),

eod_prices as (
    select *
    from ranked_prices
    where row_num = 1
),

non_null_day_prices as (
    select dim_date.date_actual as date,
           dim_date.day_of_month, dim_date.day_of_quarter, dim_date.day_of_year,
           distinct_comm.product_code,
           distinct_comm.product_name,
           case 
               when (product_eod_price is null and dim_date.date_actual = current_date)
                   then eod_prices.product_eod_price
               when product_eod_price is null 
                   then lag(eod_prices.product_eod_price, 1) 
                        over (partition by distinct_comm.product_code order by dim_date.date_actual)
               else product_eod_price
           end as product_eod_price,
           eod_prices.price_source
    from {{ ref('stg_date') }} dim_date
    cross join (select distinct product_code, product_name from eod_prices) distinct_comm
    left join eod_prices
    on dim_date.date_actual = eod_prices.date
    and distinct_comm.product_code = eod_prices.product_code
    where dim_date.date_actual <= current_date
)

select *
from non_null_day_prices
order by date desc, product_code;
