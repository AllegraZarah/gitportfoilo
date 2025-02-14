import requests
import hashlib
import time
import logging
from logging.handlers import RotatingFileHandler
import smtplib
from email.message import EmailMessage
from datetime import datetime, timedelta
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
import base64
import psycopg2
from psycopg2.extras import execute_values
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
API_KEY = os.getenv('API_KEY')
SECRET_KEY = os.getenv('SECRET_KEY')
ENCRYPTION_KEY = os.getenv('ENCRYPTION_KEY').encode()
IV = os.getenv('IV').encode()

BASE_URL = "https://api.example.com/v1"
DATA_ENDPOINT = f"{BASE_URL}/projects/metrics?include_all=true"

DB_CONFIG = {
    "dbname": os.getenv('DB_NAME'),
    "user": os.getenv('DB_USER'),
    "password": os.getenv('DB_PASSWORD'),
    "host": os.getenv('DB_HOST'),
    "port": os.getenv('DB_PORT')
}

SCHEMA = os.getenv('DB_SCHEMA')

def generate_hash(api_key, secret_key, timestamp):
    """Generate authentication hash using API credentials and timestamp"""
    message = f"{api_key}{secret_key}{timestamp}"
    return hashlib.sha256(message.encode()).hexdigest()

def decrypt_field(encrypted_value):
    """Decrypt AES-encrypted field values from the API response"""
    try:
        encrypted_data = base64.b64decode(encrypted_value)
        cipher = AES.new(ENCRYPTION_KEY, AES.MODE_CBC, IV)
        decrypted_data = unpad(cipher.decrypt(encrypted_data), AES.block_size)
        return decrypted_data.decode('utf-8')
    except Exception as e:
        logging.error(f"Decryption failed for value: {encrypted_value}. Error: {str(e)}")
        return encrypted_value

def make_api_request(endpoint, params=None):
    """Make authenticated API request and decrypt response data"""
    timestamp = str(int(time.time()))
    hash_key = generate_hash(API_KEY, SECRET_KEY, timestamp)

    headers = {
        "api-key": API_KEY,
        "hash-key": hash_key,
        "request-ts": timestamp
    }

    try:
        response = requests.get(endpoint, headers=headers, params=params)
        response.raise_for_status()

        encrypted_data = response.json()
        decrypted_data = encrypted_data.copy()

        if 'data' in decrypted_data and isinstance(decrypted_data['data'], list):
            for item in decrypted_data['data']:
                for key, value in item.items():
                    if isinstance(value, str):
                        item[key] = decrypt_field(value)
            return decrypted_data
        return None

    except requests.RequestException as e:
        logging.error(f"API request failed: {str(e)}")
        return None

def create_table_if_not_exists(cur, schema_name, table_name, data):
    """Create PostgreSQL table with appropriate schema if it doesn't exist"""
    columns = data[0].keys()
    column_defs = [f"{col} TEXT" for col in columns]
    column_defs.append("UNIQUE (project_id, metric_period)")
    
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        {', '.join(column_defs)}
    )
    """
    cur.execute(create_table_query)

def insert_data_to_db(schema_name, table_name, data):
    """Insert or update decrypted data into PostgreSQL with conflict handling"""
    data = data.get('data', [])
    if not data:
        return "No data to insert."

    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                create_table_if_not_exists(cur, schema_name, table_name, data)
                    
                columns = data[0].keys()
                query = f"""
                INSERT INTO {schema_name}.{table_name} ({','.join(columns)})
                VALUES %s 
                ON CONFLICT (project_id, metric_period) DO UPDATE 
                SET {', '.join(f"{col} = EXCLUDED.{col}" for col in columns if col not in ['project_id', 'metric_period'])}
                """  

                values = [[row[col] for col in columns] for row in data]
                execute_values(cur, query, values)
            conn.commit()
            return "Data insertion successful."

    except Exception as e:
        logging.error(f"Database insertion failed: {str(e)}")

def main():
    """Process historical project metrics data for initial database population"""
    try:
        print("Fetching historical project metrics...")
        for period_id in [1, 2, 4]:
            HISTORICAL_DATA_ENDPOINT = f"{DATA_ENDPOINT}&period_id={period_id}"
            historical_data = make_api_request(HISTORICAL_DATA_ENDPOINT)
            
            if historical_data:
                insert_data_to_db(SCHEMA, 'project_metrics', historical_data)
            else:
                print("No metrics available for the specified period")

    except Exception as e:
        error_msg = f"Script execution failed: {str(e)}"
        logging.error(error_msg)
        return error_msg

if __name__ == "__main__":
    result = main()