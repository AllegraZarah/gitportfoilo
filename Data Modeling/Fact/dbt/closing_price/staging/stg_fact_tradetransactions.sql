{{ config(materialized = 'ephemeral') }}

-- Querying data for specific commodities

select 
    *

from {{ source('public_data', 'individual_trade_transactions') }}
