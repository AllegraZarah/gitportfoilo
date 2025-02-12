{{ config (materialized = 'ephemeral') }}

SELECT trade_date,
       product,
       product_code,
       total_weight_kg,
       transaction_type,
       system_name,

       transaction_id,
       deal_timestamp,
       adjusted_deal_time,
       transaction_price,
       transaction_units,
       computed_weight_per_unit,
       asset_type,
       asset_code,
       grouped_product_name,
       asset_location,
       seller_region_code,
       buyer_region_code,
       payment_currency,

       record_id,
       trade_timestamp,
       requested_price,
       requested_units,
       weight_per_unit


from {{ source ('public', 'trading_transactions') }}