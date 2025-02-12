






-- Table: fact_client_daily_portfolio_balance
-- Purpose: Tracks daily portfolio balances for clients across different products and locations


-- DDL for fact_client_daily_portfolio_balance table creation
-- Option 1: Full table refresh (when schema remains unchanged)
DELETE FROM analytics_mart.fact_client_daily_portfolio_balance;

-- Option 2: Table recreation (when schema change Is needed)
-- DROP TABLE analytics_mart.fact_client_daily_portfolio_balance;

-- CREATE TABLE analytics_mart.fact_client_daily_portfolio_balance (
--     portfolio_date DATE,                       -- Date of the portfolio balance
--     client_id VARCHAR(25),                     -- Unique identifier for client
--     product_name TEXT,                         -- Name of the product
--     product_code TEXT,                         -- Unique product identifier
--     product_type TEXT,                         -- Type/category of product
--     location TEXT,                             -- Geographic location
--     latest_lien_volume DOUBLE PRECISION,       -- Most recent lien volume
--     latest_available_volume DOUBLE PRECISION,  -- Most recent available volume
--     latest_total_volume DOUBLE PRECISION,      -- Most recent total volume
--     latest_trans_at TIMESTAMP                  -- Timestamp of latest transaction
-- );

-- Backfill Query: Updates fact_client_daily_portfolio_balance with historic records
INSERT INTO analytics_mart.fact_client_daily_portfolio_balance (
    portfolio_date,
    client_id,
    product_name,
    product_code,
    product_type,
    location,
    latest_lien_volume,
    latest_available_volume,
    latest_total_volume,
    latest_trans_at)

-- Get base transaction data from portfolio_transactions_log
WITH base_table AS (
    SELECT product_code, location, 
           DATE(created) transaction_date, client_id,
           volume, lien_start_volume, lien_end_volume,
           available_start_volume, available_end_volume,
           total_start_volume, total_end_volume, created, updated
    FROM public.portfolio_transactions_log log
),

-- Add daily transaction indexing to identify most recent transaction per day
transaction_index AS (
    SELECT ROW_NUMBER() OVER (
        PARTITION BY transaction_date, client_id, product_code, location 
        ORDER BY created DESC
    ) daily_index,
    *
    FROM base_table
),

-- Filter to only the most recent daily transaction
indexed_table AS (
    SELECT *
    FROM transaction_index
    WHERE daily_index = 1
),

-- Generate date series for the required time period
date_dim AS (
    SELECT date_actual
    FROM analytics_mart.dim_date
    WHERE date_actual BETWEEN '2023-01-01' AND (current_date - 1)
),

-- Determine each client's portfolio start date per product
client_portfolio_start_date AS (
    SELECT client_id,
           product_code, location,
           DATE(MIN(created)) AS start_date
    FROM base_table
    GROUP BY client_id, product_code, location
),

-- Create complete date series for each client's portfolio
clients_portfolio_and_dates AS (
    SELECT client_portfolio_start_date.*,
           date_dim.date_actual portfolio_date
    FROM client_portfolio_start_date
    LEFT JOIN date_dim
    ON client_portfolio_start_date.start_date <= date_dim.date_actual
),

-- Combine daily dates with transaction data and product details
everyday_and_transaction_day_only AS (
    SELECT cpad.portfolio_date, cpad.client_id, cpad.product_code, 
           pdt.product_name, pdt.product_code, pdt.product_type, cpad.location, 
           it.volume, it.lien_start_volume, it.lien_end_volume,
           it.available_start_volume, it.available_end_volume,
           it.total_start_volume, it.total_end_volume, it.created, it.updated,
           CASE WHEN it.volume IS NULL THEN 0 ELSE 1 END transaction_boolean_identifier,
           SUM(CASE WHEN it.volume IS NULL THEN 0 ELSE 1 END) 
               OVER (PARTITION BY cpad.client_id, cpad.product_code, cpad.location 
                    ORDER BY cpad.portfolio_date) running_total_trans_day
    FROM clients_portfolio_and_dates cpad
    LEFT JOIN indexed_table it 
        ON cpad.portfolio_date = it.transaction_date
        AND cpad.client_id = it.client_id
        AND cpad.product_code = it.product_code
        AND cpad.location = it.location
    LEFT JOIN public.product pdt
        ON it.product_code = pdt.product_code
),

-- Calculate final balances using last known values
final_data AS (
    SELECT *,
           FIRST_VALUE(lien_end_volume) OVER (
               PARTITION BY client_id, product_code, location, running_total_trans_day 
               ORDER BY client_id, portfolio_date
           ) latest_lien_volume,
           FIRST_VALUE(available_end_volume) OVER (
               PARTITION BY client_id, product_code, location, running_total_trans_day 
               ORDER BY client_id, portfolio_date
           ) latest_available_volume,
           FIRST_VALUE(total_end_volume) OVER (
               PARTITION BY client_id, product_code, location, running_total_trans_day 
               ORDER BY client_id, portfolio_date
           ) latest_total_volume,
           COALESCE(created, 
                    FIRST_VALUE(created) OVER (
                        PARTITION BY client_id, product_code, location, running_total_trans_day 
                        ORDER BY client_id, portfolio_date
                    )
           ) latest_trans_at
    FROM everyday_and_transaction_day_only
)

-- Return final dataset ordered by date (newest first), client, product, and location
SELECT 'portfolio_date', 
       'client_id', 
       'product_name', 
       'product_code', 
       'product_type',
       'location', 
       'latest_lien_volume', 
       'latest_available_volume', 
       'latest_total_volume', 
       'latest_trans_at'
FROM final_data
ORDER BY portfolio_date DESC, client_id, product_code, location;