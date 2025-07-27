# Import required libraries
import configparser
import ast
from datetime import datetime, date, timedelta
import pandas as pd
from sqlalchemy import text
import os
from cred import db_conn

# Get the absolute path of the current directory
dir_path = os.path.dirname(os.path.realpath('__file__'))

# Initialize config parser and read configuration file
config = configparser.ConfigParser()
config.read(f"{dir_path}/config.ini")

# Set up database connection using credentials from config
config_source = 'ANALYTICS_SOURCE_DB'
source_engine, conn_source = db_conn(conn_param=config_source)

# Check if we already have balance records for yesterday in the database
check_query = """
SELECT COUNT(*) as count
FROM analytics_mart.fact_client_daily_portfolio_balance
WHERE DATE(portfolio_date) = (CURRENT_DATE - 1)"""
# This checks for yesterday's date because the updates for a particular day are done the following day

# Execute the check query and get the count of existing records
existing_count = pd.read_sql_query(check_query, source_engine).iloc[0]['count']

# If records exist for yesterday, delete them to avoid duplicates
if existing_count > 0:
    delete_query = """
    DELETE FROM analytics_mart.fact_client_daily_portfolio_balance
    WHERE DATE(portfolio_date) = (CURRENT_DATE - 1)
    """
    with source_engine.connect() as connection:
        connection.execute(text(delete_query))

# Query to get latest portfolio balance for all clients
# Uses CTEs (Common Table Expressions) to handle the data in steps:
# 1. base_table: Gets all portfolio transactions and joins with product information for complete product details
# 2. indexed_table: Numbers each transaction by client/product/location, ordering by creation date to identify the most recent records
# 3. Final SELECT: Filters to only the most recent transaction for each portfolio, returning the latest balance information
latest_balance = """
-- Base query to get all portfolio transactions
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

    WHERE logg.created >= '2022-08-28'
			AND (NOT (sec.product_type = 'Dawa' AND logg.location_id IS NULL ))
			AND (sec.maturity_date IS NULL -- Case 1: No maturity date (Usually non-financial market products)
				-- Case 2: Last transaction was BEFORE or ON maturity date but not more than 1 month from maturity date
                OR ((logg.created <= sec.maturity_date) AND ((sec.maturity_date + INTERVAL '1 month') >= (CURRENT_DATE - 1)))
				-- Case 2: Last transaction was AFTER maturity date but not more than 1 month from that latest creation date
				OR ((logg.created > sec.maturity_date) AND ((logg.created + INTERVAL '1 month') >= (CURRENT_DATE - 1)))
				)
	-- 2022-08-28 is the date when location_breakdown computation commenced 
	-- and using non null Dawa because all of such have been connected to locations and as such these are useless and would not enable location wise analysis
	-- and excluding securities (FI & ETC) that are 1 month post-maturity or last creation date
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
            , ROW_NUMBER() OVER (PARTITION BY client_id, product_code, location_code_after ORDER BY created DESC) daily_index

    FROM transaction_per_location
	
    WHERE volume IS NOT NULL
	)

-- Retrieve only the last transaction for each client per product and location
SELECT ind.transaction_date portfolio_date
       , cl.client_id
       , ind.product_code
       , loc.name AS "location"
       , loc.state AS "state"
       , ROUND(ind.total_volume_after, 4) AS total_portfolio_balance
       , ind.created AS latest_trans_at

FROM indexed_trans_per_loc ind
LEFT JOIN public.dim_client cl
ON ind.client_id = cl.id
LEFT JOIN public.dim_location loc
ON ind.location_code_after = loc.code

WHERE daily_index = 1

ORDER BY client_id, product_code, "location" 
"""
hot_data = pd.read_sql_query(latest_balance,source_engine)

fin_data = hot_data[['portfolio_date', 'client_id', 'product_code', 
                     'location', 'state',
					 'total_portfolio_balance', 'latest_trans_at']]

# change the latest portfolio_date to the current date
yesterday = (date.today()) - timedelta(days=1)
fin_data.portfolio_date = yesterday.strftime("%Y-%m-%d")
print('\n\n\ntoday_date :',yesterday)

# Save the final dataset to the database
# If table doesn't exist, it will be created
# If table exists, new records will be appended
fin_data.to_sql('fact_client_daily_portfolio_balance', source_engine, schema='analytics_mart', index=False, if_exists="append")
