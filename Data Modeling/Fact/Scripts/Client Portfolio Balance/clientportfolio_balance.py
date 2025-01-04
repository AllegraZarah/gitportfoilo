import configparser
from datetime import date, timedelta
import pandas as pd
from sqlalchemy import text
import os
from credentials import db_conn

# Get directory path
dir_path = os.path.dirname(os.path.realpath('__file__'))

# Load configuration
config = configparser.ConfigParser()
config.read(f"{dir_path}/config.ini")

config_source = 'MODELED_SOURCE_DB'

# Establish connection
source_engine, conn_source = db_conn(conn_param=config_source)

try:
    # Check for existing records
    check_query = """
    SELECT COUNT(*) as count
    FROM anonymized_mart.portfolio_balance_daily
    WHERE DATE(portfolio_date) = (CURRENT_DATE - 1)
    """
    existing_count = pd.read_sql_query(check_query, source_engine).iloc[0]['count']

    # Delete if records exist
    if existing_count > 0:
        delete_query = """
        DELETE FROM anonymized_mart.portfolio_balance_daily
        WHERE DATE(portfolio_date) = (CURRENT_DATE - 1)
        """
        with source_engine.connect() as connection:
            connection.execute(text(delete_query))

    # Get latest portfolio balance
    latest_balance = """
    WITH base_table AS (
        SELECT logg.*, COALESCE(region_id, 0) AS adj_region_id_,
               asset.asset_name AS asset_name, 
               asset.asset_code AS asset_code, 
               asset.asset_type AS asset_type
        FROM anonymized_logs.operation_logs logg
        LEFT JOIN anonymized_schema.dim_asset asset ON logg.asset_id = asset.id
        WHERE NOT (asset.asset_type = 'Dummy' AND logg.region_id IS NULL)
    ),
    coalesce_fi_dummy_region AS (
        SELECT *, 
               CASE 
                   WHEN asset_type = 'FI' OR (asset_type = 'Spot' AND asset_code NOT LIKE 'S%') 
                   THEN 152 
                   ELSE adj_region_id_ 
               END adj_region_id
        FROM base_table
    ),
    adjusted_region_id AS (
        SELECT logg.client_id AS client_ref, 
               DATE(logg.created) AS transaction_date,
               logg.*, 
               client.cid AS client_id, 
               region.code AS region_code, 
               region.name AS region_name, 
               region.state AS region_state
        FROM coalesce_fi_dummy_region logg
        LEFT JOIN anonymized_clients.client client ON logg.client_id = client.id
        LEFT JOIN anonymized_clients.region region ON logg.adj_region_id = region.id
    ),
    indexed_table AS (
        SELECT ROW_NUMBER() OVER (PARTITION BY client_ref, asset_id, adj_region_id ORDER BY created DESC) AS daily_index,
               *
        FROM adjusted_region_id
    )
    SELECT transaction_date, client_id, asset_name, asset_code, asset_type, region_code, region_name,
           region_state, lien_units_after AS latest_lien_units, available_units_after AS latest_available_units,
           total_units_after AS latest_total_units, created AS trans_created_at, updated AS trans_updated_at
    FROM indexed_table
    WHERE daily_index = 1
    ORDER BY transaction_date DESC
    """
    hot_data = pd.read_sql(latest_balance, source_engine)

    # Adjust portfolio_date to yesterday
    yesterday = date.today() - timedelta(days=1)
    hot_data['portfolio_date'] = yesterday.strftime("%Y-%m-%d")

    # Write data to the target table
    hot_data.to_sql('portfolio_balance_daily', source_engine, schema='anonymized_mart', index=False, if_exists="append", method='multi')
    print(f"Data successfully inserted for date: {yesterday}")

except Exception as e:
    print(f"An error occurred: {e}")
finally:
    source_engine.dispose()
