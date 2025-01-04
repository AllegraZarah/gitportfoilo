{{ config(materialized = 'ephemeral') }}

with 
historical_sec_prices as (
    select price.*,
           price.asset_code as adj_asset_code,
           dim_date.day_of_month, 
           dim_date.day_of_quarter, 
           dim_date.day_of_year
    from {{ source('public_data', 'historical_prices') }} price
    left join {{ source('public_data', 'date_dimension') }} dim_date
    on date(price.date) = date(dim_date.date_actual)
)

select *
from historical_sec_prices
