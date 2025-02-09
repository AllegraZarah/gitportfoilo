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
FROM analytics_mart.fact_client_daily_balance
WHERE DATE(date) = (CURRENT_DATE - 1)"""
# This checks for yesterday's date because the updates for a particular day are done the following day

# Execute the check query and get the count of existing records
existing_count = pd.read_sql_query(check_query, source_engine).iloc[0]['count']

# If records exist for yesterday, delete them to avoid duplicates
if existing_count > 0:
    delete_query = """
    DELETE FROM analytics_mart.fact_client_daily_balance
    WHERE DATE(date) = (CURRENT_DATE - 1)
    """
    with source_engine.connect() as connection:
        connection.execute(text(delete_query))

# Query to get the most recent balance for each client
# Uses a CTE (Common Table Expression) to handle the data in steps:
# 1. base_table: Gets all valid transactions (excluding Declined and Reverted)
# 2. indexed_table: Numbers each transaction per client, with most recent first
# 3. Final SELECT: Gets only the most recent transaction per client
latest_balance = """
    WITH base_table as (
        SELECT trans.client_id AS id_index
                , DATE(trans.created_at) AS date
                , trans.*
                
        FROM public.transactions_log
        WHERE transaction_status NOT IN ('Declined', 'Reverted')
    ),
    
    indexed_table as (
        SELECT ROW_NUMBER() OVER (PARTITION BY id_index ORDER BY created_at desc) AS daily_index,
                *
        FROM base_table 
    )

    SELECT date
            , client_id
            , end_amount as account_balance
            , created_at as latest_trans_at
    FROM indexed_table
    WHERE daily_index = 1
"""

# Execute the balance query and store results in DataFrame
hot_data = pd.read_sql_query(latest_balance, source_engine)

# Select only the required columns for the final dataset
fin_data = hot_data[['date',
                    'client_id',
                    'account_balance',
                    'latest_trans_at']]

# Update all dates to yesterday's date
yesterday = (date.today()) - timedelta(days=1)
fin_data.date = yesterday.strftime("%Y-%m-%d")
print('\n\n\ntoday_date :', yesterday)

# Save the final dataset to the database
# If table doesn't exist, it will be created
# If table exists, new records will be appended
fin_data.to_sql('fact_client_daily_balance', source_engine, schema='analytics_mart', index=False, if_exists="append")