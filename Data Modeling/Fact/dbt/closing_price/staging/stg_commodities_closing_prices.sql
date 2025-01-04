{{ config(materialized = 'ephemeral') }}

with
price as (
    -- Retrieve the commodities' closing prices
    -- The upper part of the union includes closing prices from an external source not in the database.
    -- Data before a specific date is sourced externally, while on-system data is used thereafter.
    
    select date(date) as date,
           case 
               when product = 'productA' then 'CMA'
               when product = 'productB' then 'CMB'
               when product = 'productC' then 'CMC'
               else product
           end as product_code,
           case 
               when product = 'productA' then 'product A Description'
               when product = 'productB' then 'product B Description'
               when product = 'productC' then 'product C Description'
               else product
           end as product_name,
           (price * 10) / 1000 as product_eod_price, -- Adjusted price
           'external_source' as source
    from {{ source('external_data', 'historical_prices') }}

    UNION

    select price.date,
           case when price.product_code = 'CODEX' then 'CODEY' else price.product_code end as product_code,
           comm.name as product_name,
           price.price as product_eod_price,
           'internal_source' as source
    from {{ source('internal_data', 'product_prices') }} price
    left join {{ source('internal_data', 'product_metadata') }} comm
    on price.product_code = comm.code
    where price.price != 0
),

ranked_prices as (
    select *,
           row_number() over (partition by date, product_code order by date, source) as row_num
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
           eod_prices.source
    from {{ source('public', 'date_dimension') }} dim_date -- Using a public date dimension
    cross join (select distinct product_code, product_name from eod_prices) distinct_comm
    left join eod_prices
    on dim_date.date_actual = eod_prices.date
    and distinct_comm.product_code = eod_prices.product_code
    where dim_date.date_actual <= current_date
)

select *
from non_null_day_prices
order by date desc, product_code;
