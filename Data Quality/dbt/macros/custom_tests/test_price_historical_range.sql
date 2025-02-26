{% test price_within_historical_range(model, column_name) %}

with historical_stats as (
    select
        avg(price) as avg_price,
        stddev(price) as price_std,
        avg(price) + (3 * stddev(price)) as upper_bound,
        avg(price) - (3 * stddev(price)) as lower_bound
    from {{ source('public', 'historical_product_prices') }}
),
current_outliers as (
    select 
        *
    from {{ model }}
    cross join historical_stats
    where {{ column_name }} > upper_bound
        or {{ column_name }} < lower_bound
)
select * from current_outliers
{% endtest %}