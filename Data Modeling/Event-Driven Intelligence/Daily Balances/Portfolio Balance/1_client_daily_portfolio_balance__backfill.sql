-- Table: fact_client_daily_portfolio_balance
-- Purpose: Tracks daily portfolio balances for clients across different products and locations


-- DDL for fact_client_daily_portfolio_balance table creation
-- Option 1: Full table refresh (when schema remains unchanged)
DELETE FROM analytics_mart.fact_client_daily_portfolio_balance;

-- Option 2: Table recreation (when schema change Is needed)
-- DROP TABLE analytics_mart.fact_client_daily_portfolio_balance;

-- CREATE TABLE public.fact_client_dailyportfoliobalance (
-- 				portfolio_date DATE,
-- 				client_id VARCHAR(25),  
-- 				product_code TEXT,
-- 				"location" TEXT,
-- 				"state" TEXT,
-- 				total_portfolio_balance INTEGER, 
-- 				latest_trans_at TIMESTAMP,
-- 				PRIMARY KEY (portfolio_date, client_id, product_code, "location")
-- 	);

-- Backfill Query: Updates fact_client_daily_portfolio_balance with historic records
INSERT INTO analytics_mart.fact_client_daily_portfolio_balance (portfolio_date
														        , client_id
														        , product_code
														        , "location"
														        , "state"
														        , total_portfolio_balance
														        , latest_trans_at)

WITH base_table AS (

    SELECT sec.product_type, sec.product_code, sec.maturity_date product_maturity_date, DATE(logg.created) transaction_date, logg.client_id,
            logg.transaction_type, logg.extra_note, logg.product_id, logg.location_id, loc.code location_code,
            logg.units, logg.lien_units_before, logg.lien_units_after, logg.available_units_before, logg.available_units_after,
            logg.total_units_before, logg.total_units_after, logg.created, logg.updated, logg.location_breakdown, logg.location_breakdown_before, logg.location_breakdown_after
    
    FROM public.portfolio_transactions_log logg
    LEFT JOIN public.dim_product sec
    ON logg.product_id = sec.id
	LEFT JOIN public.dim_location loc
	ON logg.location_id = loc.id

    WHERE logg.created >= '2022-08-28' AND (NOT (sec.product_type = 'Dawa' AND logg.location_id IS NULL ))
	-- 2022-08-28 is the date when location_breakdown computation commenced 
	-- and using non null Dawa because all of such have been connected to locations and as such these are useless and would not enable location wise analysis
    ),
    

-- The next phase here is deriving the location and portfolio balance pre and post each non-Dawa tranaction, and the associated volume moving. Essentially, we are expanding the table by location breakdown.
using_location_breakdown AS (
    SELECT *
            , string_to_array(REPLACE(TRIM(BOTH '[]' FROM location_breakdown),'},', '};'), '; ') edited_location_breakdown
            , string_to_array(REPLACE(TRIM(BOTH '[]' FROM location_breakdown_before),'},', '};'), '; ') edited_location_breakdown_before
            , string_to_array(REPLACE(TRIM(BOTH '[]' FROM location_breakdown_after),'},', '};'), '; ') edited_location_breakdown_after
    
    FROM base_table
    
    ),
    
unnested_amount AS(
    SELECT *,
            UNNEST(edited_location_breakdown) AS loc_breakdown	
            
    FROM using_location_breakdown
    ),

amount_moving AS(
    SELECT client_id, product_code, created
        -- Extract from current breakdown
        , (loc_breakdown::json)->>'volume' AS volume
        , (loc_breakdown::json)->>'location_code' AS location_code
    
    FROM unnested_amount
    ),

unnested_before_after AS(
    SELECT *,
            UNNEST(coalesce(edited_location_breakdown_before, '{"{}"}')) AS loc_breakdown_before,
            UNNEST(coalesce(edited_location_breakdown_after, '{"{}"}')) AS loc_breakdown_after		
            
    FROM using_location_breakdown
    ),

before_after_values AS(
    SELECT transaction_date, client_id, transaction_type, extra_note, product_code, product_type, product_maturity_date
            , units, lien_units_before, lien_units_after, available_units_before, available_units_after
            , total_units_before, total_units_after, created, updated, location_code

			-- For Dawa, use the actual total_units and not that in breakdown because of those that fail to compute properly in the location breakdown for Dawa, 
			-- e.g: CSD to Dawa conversion and Debit to Suspense
	
            -- Extract from before breakdown. 
            , CASE WHEN product_type = 'Dawa' THEN total_units_before * 1000
				WHEN product_type != 'Dawa' AND loc_breakdown_before = '{}' THEN total_units_before
				ELSE ((loc_breakdown_before::json)->>'volume') :: INTEGER
				END AS total_volume_before
	
            , CASE WHEN product_type = 'Dawa' THEN location_code
				WHEN product_type != 'Dawa' AND loc_breakdown_before = '{}' THEN location_code
				ELSE (loc_breakdown_before::json)->>'location_code'
				END AS location_code_before

	
            -- Extract from after breakdown
            , CASE WHEN product_type = 'Dawa' THEN total_units_after * 1000
				WHEN product_type != 'Dawa' AND loc_breakdown_after = '{}' THEN total_units_after
				ELSE ((loc_breakdown_after::json)->>'volume') :: INTEGER
				END AS total_volume_after
	
            , CASE WHEN product_type = 'Dawa' THEN location_code
				WHEN product_type != 'Dawa' AND loc_breakdown_after = '{}' THEN location_code
				ELSE (loc_breakdown_after::json)->>'location_code'
				END AS location_code_after	

    FROM unnested_before_after
    ),

transaction_per_location AS(
    SELECT bav.*
            , CASE WHEN bav.product_type = 'Dawa' THEN bav.units * 1000
				WHEN bav.product_type != 'Dawa' AND am.volume IS NULL THEN bav.units
				ELSE am.volume :: INTEGER
				END volume
	
			, CASE WHEN bav.product_type = 'Dawa' THEN bav.location_code
				WHEN bav.product_type != 'Dawa' AND am.location_code IS NULL THEN bav.location_code
				ELSE am.location_code
				END actual_location_code
    
    FROM before_after_values bav
    LEFT JOIN amount_moving am
    ON bav.client_id = am.client_id
        AND bav.location_code_after = am.location_code
        AND bav.product_code = am.product_code
        AND bav.created = am.created
    ),

indexed_trans_per_loc AS(
    SELECT *
            , ROW_NUMBER() OVER (PARTITION BY client_id, product_code, location_code_after, transaction_date ORDER BY created DESC) row_index

    FROM transaction_per_location
	
    WHERE volume IS NOT NULL
),
    
balances_per_trans AS(
    -- Retrieve only the last transaction for each client per day
    SELECT *
    FROM indexed_trans_per_loc
    WHERE row_index = 1
    ),
    
date_dim AS (
        -- Generate a date range for the required period (adjustable based on business needs)
    SELECT date_actual
    FROM public.dim_date
    WHERE date_actual BETWEEN '2022-08-28' AND (CURRENT_DATE - 1) 	--2022-08-28 is the date when location_breakdown computation commenced
    ),

portfolio_start_date AS (
        -- Determine the first transaction date for each client, product and location
    SELECT client_id
            , product_code
            , location_code_after
            , DATE(product_maturity_date) product_maturity_date
            , DATE(MIN(created)) AS start_date
			, DATE(MAX(created)) AS end_date
    
    FROM transaction_per_location
    GROUP BY client_id, product_code, location_code_after, DATE(product_maturity_date)
    ),

portfolio_and_dates AS (
        -- Generate a date series for each client, starting from their first transaction date
    SELECT portfolio_start_date.*
            , date_dim.date_actual
    
    FROM portfolio_start_date
    LEFT JOIN date_dim
    ON portfolio_start_date.start_date <= date_dim.date_actual -- Base condition: date_actual must be >= start_date
        AND (portfolio_start_date.product_maturity_date IS NULL -- Case 1: No maturity date (Usually non-financial market products)
				-- Case 2: Last transaction was BEFORE or ON maturity date
                OR ((portfolio_start_date.end_date <= portfolio_start_date.product_maturity_date) AND (date_dim.date_actual <= (portfolio_start_date.product_maturity_date + INTERVAL '1 month')))
				-- Case 3: Last transaction was AFTER maturity date
				OR ((portfolio_start_date.end_date > portfolio_start_date.product_maturity_date) AND (date_dim.date_actual <= (portfolio_start_date.end_date + INTERVAL '1 month'))))
    ),

balance_per_day AS(
    SELECT  pd.date_actual portfolio_date
            , pd.client_id
            , pd.product_code
            , pd.location_code_after
            , pd.product_maturity_date
            , bal.transaction_type
            , bal.volume
            , bal.total_volume_before
            , bal.total_volume_after
            , bal.transaction_date
            , bal.created
            , SUM(CASE WHEN bal.volume IS NULL THEN 0 ELSE 1 END)
                    OVER (PARTITION BY pd.client_id, pd.product_code, pd.location_code_after
                             ORDER BY pd.date_actual) running_total_transactions
    
    FROM portfolio_and_dates pd
    LEFT JOIN balances_per_trans bal
        ON pd.date_actual = bal.transaction_date
        AND pd.client_id = bal.client_id
        AND pd.location_code_after = bal.location_code_after
        AND pd.product_code = bal.product_code
    ),

unique_identifier AS (
        -- Add a unique identifier to associate transactions with no-transaction days to the last known balance
    SELECT *
            , CONCAT(client_id, '_', product_code, '_', location_code_after, '_', running_total_transactions) unique_balance_identifier
                -- Here is the unique identifier to identify each transaction day to enable the reversion to the last known balance, for days where there are no transactions

    FROM balance_per_day
        ),

final AS (
        -- Compute the account balance for all days, using the last known balance for days without transactions
    SELECT ui.portfolio_date
            , cl.client_id
            , ui.product_code
            , loc.name AS "location"
            , loc.state AS "state"
            , ROUND(FIRST_VALUE(ui.total_volume_after) OVER (PARTITION BY ui.unique_balance_identifier
                                                 ORDER BY ui.transaction_date), 4) total_portfolio_balance
            , FIRST_VALUE(ui.created) OVER (PARTITION BY ui.unique_balance_identifier
                                                 ORDER BY ui.transaction_date) latest_trans_at

    FROM unique_identifier ui
    LEFT JOIN public.crm_client cl
    ON ui.client_id = cl.id
    LEFT JOIN public.crm_location loc
    ON ui.location_code_after = loc.code
   )

SELECT *
FROM final
ORDER BY client_id, product_code, "location", portfolio_date DESC
