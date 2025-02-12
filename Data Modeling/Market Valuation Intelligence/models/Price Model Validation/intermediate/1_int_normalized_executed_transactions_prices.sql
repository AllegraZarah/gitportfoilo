{{ config (materialized = 'ephemeral') }}

-- This intermediate model processes executed market transactions, normalizing prices and filtering out outliers.
-- It ensures consistency in price adjustments and prepares the data for further aggregation or analysis.

with 
reconciled_market_trades as (
    -- Extract unique executed market transactions, adjusting timestamps for consistency.
    select distinct transaction_id, deal_timestamp, adjusted_deal_time,
            transaction_price transaction_price_per_unit, transaction_units,
            transaction_price / computed_weight_per_unit transaction_price_per_kg, 
            transaction_units * computed_weight_per_unit transaction_total_volume_kg, 
            asset_type, asset_code,
            product_code, product_name, grouped_product_name,
            asset_location, seller_region_code, buyer_region_code,
            case 
                when buyer_region_code is null then asset_location 
                else buyer_region_code 
            end final_destination -- Determines the final destination of the product
    from {{ ref('stg_fact_marketflows') }}
    where transaction_id is not null
            and transaction_type = 'Purchase'
            and currency = 'USD'
            and (platform_name = 'BASE')
            and asset_type in ('Forward', 'Futures', 'Options')
            and grouped_product_name in ('Rice', 'Cotton', 'Barley', 'Coffee', 'Tea', 'Turmeric',
                                            'Palm Oil', 'Peanuts', 'Corn')
),

inventory_registry as (
    -- Extract and categorize inventory data from stores.
    select date(inv.entry_date) record_date, inv.item_id, inv.item_label, 
            concat('INV', inv.item_id) listing_code, 
            inv.grade_level, inv.total_weight,
            inv.price_per_ton, inv.price_per_ton / 1000 as price_per_kg, 
            inv.trade_category, inv.receipt_id,
            store.region final_destination,
            case 
                when inv.item_id like '%CTN%' then 'Cotton'
                when inv.item_id like '%COF%' then 'Coffee'
                when inv.item_id like '%TEA%' then 'Tea'
                when inv.item_id like '%RIC%' then 'Rice'
                when inv.item_id like '%PLM%' then 'Palm Oil'
                when inv.item_id like '%PNT%' then 'Peanuts'
                when inv.item_id like '%BRL%' then 'Barley'
                when inv.item_id like '%TRM%' then 'Turmeric'
                when inv.item_id like '%CRN%' then 'Corn'
                else inv.item_id
            end grouped_product_name -- Standardizes product classification
    from {{ source ('public', 'fact_inventory') }} inv
    left join {{ source ('public', 'dim_stores') }} store
        on inv.storage_id = store.storage_id
    where item_category = 'Agri-Product'
            and inv.tenant_key = '8'
            and inv.is_verified is true
            and inv.approval_status is true
            and inv.is_canceled is false
            and inv.trade_category = 'Wholesale'
            and (inv.item_id like '%CTN%' or inv.item_id like '%COF%' or inv.item_id like '%TFA%' or 
                 inv.item_id like '%TEA%' or inv.item_id like '%RIC%' or inv.item_id like '%PLM%' or 
                 inv.item_id like '%PNT%' or inv.item_id like '%GRN%' or inv.item_id like '%BRL%' or 
                 inv.item_id like '%TRM%' or inv.item_id like '%CRN%')
),
	
combined_market_inventory as (
    -- Combine inventory records with executed market transactions.
    select record_date, grouped_product_name, item_label, item_id, listing_code, 
           'Store Receipt' as asset_type, receipt_id as record_id, total_weight as volume_kg, 
           price_per_kg, final_destination
    from inventory_registry
    
    union
    
    select adjusted_deal_time, grouped_product_name, product_name, product_code, 
           asset_code, asset_type, transaction_id as record_id, transaction_total_volume_kg, 
           transaction_price_per_kg, final_destination
    from reconciled_market_trades
),

adjusted_market_inventory_prices as (
    -- Adjust market inventory prices based on transportation cost differentials.
    select price_data.*, logistics.cost_adjustment_per_kg, 
           (price_data.price_per_kg + coalesce(logistics.cost_adjustment_per_kg, 0)) as adjusted_price,
           price_data.volume_kg * (price_data.price_per_kg + coalesce(logistics.cost_adjustment_per_kg, 0)) as adjusted_value
    from combined_market_inventory price_data
    left join {{ ref ('stg_transport_matrix') }} logistics
        on price_data.grouped_product_name = logistics.product_label
        and price_data.final_destination = logistics.destination_region
),

price_band_analysis as (
    -- Compute the average and standard deviation for adjusted market inventory prices per product per day.
    select *, 
           avg(adjusted_price) over (partition by listing_code, record_date order by record_date) as avg_price_per_kg,
           coalesce(stddev(adjusted_price) over (partition by listing_code, record_date order by record_date), 0) as std_dev_price_per_kg,
           (avg(adjusted_price) over (partition by listing_code, record_date order by record_date) 
           - coalesce(stddev(adjusted_price) over (partition by listing_code, record_date order by record_date), 0)) as lower_band,
           (avg(adjusted_price) over (partition by listing_code, record_date order by record_date) 
           + coalesce(stddev(adjusted_price) over (partition by listing_code, record_date order by record_date), 0)) as upper_band
    from adjusted_market_inventory_prices
),

outlier_filtered_inventory as (
    -- Remove outliers that fall outside of the calculated price bands.
    select *
    from price_band_analysis
    where adjusted_price between lower_band and upper_band
)

-- Final selection of the cleaned and normalized executed market transactions.
select *
from outlier_filtered_inventory