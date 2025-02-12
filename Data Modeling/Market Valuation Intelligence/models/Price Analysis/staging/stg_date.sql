{{ config (materialized = 'ephemeral') }}

with 
date_dim as (
    select date_actual

    from {{source ('public', 'dim_date')}}
    )

select *

from date_dim