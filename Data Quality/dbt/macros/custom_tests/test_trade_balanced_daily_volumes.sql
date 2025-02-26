{% test balanced_daily_volumes(model, buy_volume_column, sell_volume_column, tolerance=0.1) %}

with daily_volumes as (
    select
        date,
        sum(case when transaction_type = 'BUY' then {{ buy_volume_column }} else 0 end) as buy_volume,
        sum(case when transaction_type = 'SELL' then {{ sell_volume_column }} else 0 end) as sell_volume
    from {{ model }}
    group by date
)
select *
from daily_volumes
where abs(buy_volume - sell_volume) / nullif(greatest(buy_volume, sell_volume), 0) > {{ tolerance }}
{% endtest %}