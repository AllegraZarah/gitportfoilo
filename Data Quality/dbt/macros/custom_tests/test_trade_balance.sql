{% test balanced_trades(model, buy_volume_column, sell_volume_column, tolerance_percentage=1) %}

with trade_balance as (
    select 
        date,
        sum({{ buy_volume_column }}) as total_buy_volume,
        sum({{ sell_volume_column }}) as total_sell_volume,
        abs(sum({{ buy_volume_column }}) - sum({{ sell_volume_column }})) / 
        nullif(sum({{ buy_volume_column }}), 0) * 100 as imbalance_percentage
    from {{ model }}
    group by date
)

select *
from trade_balance
where imbalance_percentage > {{ tolerance_percentage }}

{% endtest %}