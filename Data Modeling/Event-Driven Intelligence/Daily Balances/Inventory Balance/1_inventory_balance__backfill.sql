-- Table: fact_store_dailyinventorylevels
-- Purpose: Tracks daily inventory of each product at each store

-- DDL for fact_client_daily_balance table creation
-- Option 1: Full table refresh (when schema remains unchanged)
DELETE FROM analytics_mart.fact_store_dailyinventorylevels

-- Option 2: Table recreation (when schema changes needed)
-- -- -- Create fact_store_dailyinventorylevels Table
-- DROP TABLE analytics_mart.fact_store_dailyinventorylevels;

CREATE TABLE analytics_mart.fact_store_dailyinventorylevels (
    inventory_date DATE,
    store_id VARCHAR(25),
    store_name TEXT,
    product_id VARCHAR(25),
    product_name TEXT,
    product_code TEXT,
    product_category VARCHAR(25),
    quality_level VARCHAR(25),
    client_id VARCHAR(25),
    current_reserved_units INT,
    current_available_units INT,
    current_total_units INT,
    current_reserved_volume DOUBLE PRECISION,
    current_available_volume DOUBLE PRECISION,
    current_total_volume DOUBLE PRECISION,
    store_inventory_account_id VARCHAR(25),
    latest_trans_at TIMESTAMP
);

-- Backfill Query: Updates fact_store_dailyinventorylevels with historic records
INSERT INTO analytics_mart.fact_store_dailyinventorylevels (
    inventory_date, store_id, store_name, product_id, product_name, product_code, 
    product_category, quality_level, client_id, 
    current_reserved_units, current_available_units, current_total_units,
    current_reserved_volume, current_available_volume, current_total_volume,
    store_inventory_account_id, latest_trans_at)

-- Create an index to identify each store's product and quality level
with base_table AS (
    SELECT concat(acc.store_id, acc.product_id, acc.quality_level) id_index,
        acc.store_id,
        acc.product_id,
        acc.quality_level,
        trans.created created_date,
        trans.*
    
    FROM inventory.store_inventory_transaction trans
    LEFT JOIN inventory.store_inventory_account acc
    ON trans.store_inventory_account_id = acc.id
    
    WHERE trans.is_deleted is false
),

-- Create another index to identify daily transactions
transaction_index AS (
    SELECT row_number() OVER (PARTITION BY created_date, id_index ORDER BY created desc) daily_index, *
    FROM base_table
),

-- Get last transaction of each store per day per product and quality level
indexed_table AS (
    SELECT *
    FROM transaction_index
    WHERE daily_index = 1
),

date_dim AS (
        -- Generate a date range for the required period (adjustable based ON business needs)
	SELECT date_actual
	
	FROM analytics_mart.dim_date
    
    WHERE date_actual between '2013-01-01' AND (current_date - 1)
	),

inventory_start_date AS (
        -- Determine the first transaction date for store and product
    SELECT store_id,
            store_inventory_account_id,
            client_id
            product_id,
            quality_level,
        	DATE(MIN(created_at)) AS start_date
	
    FROM base_table
    GROUP BY store_id, store_inventory_account_id, client_id product_id, quality_level
	),

inventory_and_dates AS (
        -- Generate a date series for each store and product, starting FROM their first transaction date
    SELECT inventory_start_date.*
            , date_dim.date_actual AS inventory_date
    
    FROM inventory_start_date
    LEFT JOIN date_dim
    ON inventory_start_date.start_date <= date_dim.date_actual
    ),

inventory_per_day AS (
        -- Join all dates with transactions and create running totals
    SELECT iad.*, 
        store.name store_name, 
        prod_item.name AS product_name, prod_item.code AS product_code, prod_item.category_type AS product_category,
        indexed_table.created_date,
        indexed_table.units, indexed_table.reserved_units_before, indexed_table.reserved_units_after,
        indexed_table.units_before, indexed_table.units_after,
        indexed_table.total_units_before, indexed_table.total_units_after,
        indexed_table.volume, indexed_table.reserved_volume_before, indexed_table.reserved_volume_after,
        indexed_table.volume_before, indexed_table.volume_after,
        indexed_table.total_volume_before, indexed_table.total_volume_after

        CASE WHEN volume IS NULL THEN 0 ELSE 1 END transaction_boolean_identifier,
        SUM(CASE WHEN volume IS NULL THEN 0 ELSE 1 END) 
            OVER (PARTITION BY iad.store_id, iad.product_id, iad.quality_level ORDER BY iad.date_actual) running_total

    FROM inventory_and_dates iad

    LEFT JOIN inventory.store store
    ON iad.store_id = store.id
    
    LEFT JOIN analytics_mart.dim_product prod_item
    ON iad.product_id = prod_item.id

    LEFT JOIN indexed_table 
    ON iad.date_actual = date(indexed_table.created_date)
        AND iad.store_id = indexed_table.store_id
        AND iad.product_id = indexed_table.product_id
        AND iad.quality_level = indexed_table.quality_level


-- Calculate final values using window functions
final_data AS (
    SELECT inventory_date, store_id, store_name, product_id, product_name, product_code,
        product_category, quality_level, client_id,
        first_value(reserved_units_after) OVER w AS current_reserved_units,
        first_value(units_after) OVER w AS current_available_units,
        first_value(total_units_after) OVER w AS current_total_units,
        first_value(reserved_volume_after) OVER w AS current_reserved_volume,
        first_value(volume_after) OVER w AS current_available_volume,
        first_value(total_volume_after) OVER w AS current_total_volume,
        store_inventory_account_id,
        coalesce(created_date, (first_value(created_date) OVER w)) latest_trans_at
    FROM inventory_per_day
    window w AS (PARTITION BY store_id, product_id, quality_level, running_total_trans_day ORDER BY created_date)
)

SELECT *
FROM final_data
ORDER BY inventory_date desc, store_name, product_code, quality_level