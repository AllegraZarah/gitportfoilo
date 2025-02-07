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
config_source = 'SOURCE_DB'
source_engine, conn_source = db_conn(conn_param=config_source)

# Check if we already have balance records for yesterday in the database
check_query = """
SELECT COUNT(*) as count
FROM public_mart.fact_client_daily_portfolio_balance
WHERE DATE(portfolio_date) = (CURRENT_DATE - 1)"""
# This checks for yesterday's date because the updates for a particular day are done the following day

# Execute the check query and get the count of existing records
existing_count = pd.read_sql_query(check_query, source_engine).iloc[0]['count']

# If records exist for yesterday, delete them to avoid duplicates
if existing_count > 0:
    delete_query = """
    DELETE FROM public_mart.fact_client_daily_portfolio_balance
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
    SELECT product_code, 
           location, 
           client_id,
           volume, 
           lien_start_volume, 
           lien_end_volume,
           available_start_volume, 
           available_end_volume,
           total_start_volume, 
           total_end_volume, 
           created, 
           updated,
           -- Adding portfolio_date from created timestamp for ordering
           DATE(created) as portfolio_date,
           -- Join with product table to get product details
           p.product_name,
           p.product_type
    FROM public.portfolio_transactions_log log
    LEFT JOIN public.product p ON log.product_code = p.product_code
),

-- Get the most recent transaction for each client/product/location combination
indexed_table AS (
    SELECT *, 
           ROW_NUMBER() OVER (
               PARTITION BY client_id, product_code, location 
               ORDER BY created DESC
           ) daily_index
    FROM base_table 
)

-- Select the latest balance information for each portfolio
SELECT portfolio_date, 
       client_id, 
       product_name, 
       product_code, 
       product_type,
       location, 
       lien_end_volume AS latest_lien_volume, 
       available_end_volume AS latest_available_volume, 
       total_end_volume AS latest_total_volume, 
       created AS latest_trans_at
FROM indexed_table
WHERE daily_index = 1
ORDER BY portfolio_date DESC;
"""

# Execute query and fetch results
cur = conn_source.execute(text(latest_balance))
res = cur.fetchall()

# Convert results to pandas DataFrame
hot_data = pd.DataFrame(res)

# Select required columns in specific order for final output
fin_data = hot_data[[
    'portfolio_date',
    'client_id',
    'product_name',
    'product_code',
    'product_type',
    'location',
    'latest_lien_volume',
    'latest_available_volume',
    'latest_total_volume',
    'latest_trans_at'
]]

# Update all dates to yesterday's date
yesterday = (date.today()) - timedelta(days=1)
fin_data.portfolio_date = yesterday.strftime("%Y-%m-%d")
print('\n\n\ntoday_date :',yesterday)

# Save the final dataset to the database
# If table doesn't exist, it will be created
# If table exists, new records will be appended
fin_data.to_sql('fact_client_daily_portfolio_balance', source_engine, schema='public_mart', index=False, if_exists="append")