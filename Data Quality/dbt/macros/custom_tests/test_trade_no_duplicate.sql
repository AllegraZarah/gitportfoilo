{% test no_duplicate_trades(model, trade_id_column, trade_timestamp_column) %}

select
    {{ trade_id_column }},
    {{ trade_timestamp_column }},
    count(*) as occurrence_count
from {{ model }}
group by {{ trade_id_column }}, {{ trade_timestamp_column }}
having count(*) > 1
{% endtest %}