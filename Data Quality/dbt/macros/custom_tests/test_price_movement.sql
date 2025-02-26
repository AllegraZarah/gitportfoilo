{% test reasonable_price_movement(model, column_name, max_movement_percentage=20) %}

with price_changes as (
    select 
        date,
        {{ column_name }},
        lag({{ column_name }}) over (order by date) as previous_price,
        abs({{ column_name }} - lag({{ column_name }}) over (order by date)) / 
        nullif(lag({{ column_name }}) over (order by date), 0) * 100 as price_change_pct
    from {{ model }}
)

select *
from price_changes
where price_change_pct > {{ max_movement_percentage }}
  and previous_price is not null

{% endtest %}