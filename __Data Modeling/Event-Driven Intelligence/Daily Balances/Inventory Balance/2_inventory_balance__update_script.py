import configparser
import ast
from datetime import datetime, date, timedelta
import pandas as pd
import os
from credentials import db_conn
from datetime import datetime
dir_path = os.path.dirname(os.path.realpath('__file__'))
import psycopg2

config = configparser.ConfigParser()
config.read(f"{dir_path}/config.ini")

config_source = 'ANALYTICS_SOURCE_DB'

# connection
source_engine, conn_source = db_conn(conn_param=config_source)

# Define the query to check for existing records for today's date
check_query = """
SELECT COUNT(*) as count
FROM analytics_mart.fact_store_dailyinventorylevels
WHERE DATE(inventory_date) = (CURRENT_DATE - 1)
"""

# Execute the check query
try:
    existing_count = pd.read_sql_query(check_query, source_engine).iloc[0]['count']
except psycopg2.errors.UndefinedTable:
    existing_count = 0

# If there are existing records for today, delete them
if existing_count > 0:
    delete_query = """
    DELETE FROM analytics_mart.fact_store_dailyinventorylevels
    WHERE DATE(inventory_date) = (CURRENT_DATE - 1)
    """
    with source_engine.connect() as connection:
        connection.execute(delete_query)

# Get the latest inventory levels for all stores
latest_inventory = """
with base_table as (
    select concat(acc.store_id, acc.product_id, acc.quality_level) id_index,
            acc.store_id,
            store.name as store_name,
            acc.product_id,
            prod_item.name as product_name,
            prod_item.code as product_code,
            prod_item.category_type as product_category,
            acc.quality_level,
            DATE(trans.created) inventory_date,
            trans.*
    from inventory.store_inventory_transaction trans
    left join inventory.store_inventory_account acc
    on trans.store_inventory_account_id = acc.id
    left join inventory.store store
    on acc.store_id = store.id
    left join analytics_mart.dim_product prod_item
    on acc.product_id = prod_item.id
),

indexed_table as (
    select row_number() over (partition by id_index order by created desc) daily_index, *
    from base_table
)

select inventory_date, store_id, store_name, product_id, product_name, product_code, 
        product_category, quality_level, client_id,
        reserved_units_after as current_reserved_units,
        units_after as current_available_units,
        total_units_after as current_total_units,
        reserved_volume_after as current_reserved_volume,
        volume_after as current_available_volume,
        total_volume_after as current_total_volume,
        store_inventory_account_id,
        created latest_trans_at

from indexed_table
where daily_index = 1
order by created desc
"""

current_data = pd.read_sql_query(latest_inventory, source_engine)

final_data = current_data[['inventory_date', 'store_id', 'store_name', 'product_id',
                          'product_name', 'product_code', 'product_category', 'quality_level',
                          'client_id', 'current_reserved_units', 'current_available_units',
                          'current_total_units', 'current_reserved_volume',
                          'current_available_volume', 'current_total_volume',
                          'store_inventory_account_id', 'latest_trans_at']]

# Set inventory date to yesterday
yesterday = (date.today()) - timedelta(days=1)
final_data.inventory_date = yesterday.strftime("%Y-%m-%d")
print('\n\n\ntoday_date:', yesterday)

# Save to analytics_mart schema
final_data.to_sql('fact_store_dailyinventorylevels', source_engine,
                  schema='analytics_mart', index=False, if_exists="append")