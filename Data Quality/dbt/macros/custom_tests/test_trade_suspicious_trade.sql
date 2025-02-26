{% test suspicious_trading_volume(model, volume_column, avg_window=30) %}

with volume_stats as (
    select
        date,
        {{ volume_column }},
        avg({{ volume_column }}) over 
            (order by date rows between {{ avg_window }} preceding and 1 preceding) as avg_volume,
        stddev({{ volume_column }}) over 
            (order by date rows between {{ avg_window }} preceding and 1 preceding) as vol_std
    from {{ model }}
)
select *
from volume_stats
where {{ volume_column }} > (avg_volume + (3 * vol_std))
    and avg_volume is not null
{% endtest %}