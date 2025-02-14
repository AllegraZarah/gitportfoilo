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
API_KEY = os.getenv('YOUR_API_KEY')
SECRET_KEY = os.getenv('YOUR_SECRET_KEY')
ENCRYPTION_KEY = os.getenv('ENCRYPTION_KEY').encode()
IV = os.getenv('IV').encode()

BASE_URL = "https://api.example.com/v1"
TEAM_ENDPOINT = f"{BASE_URL}/analytics/team-composition?include_all=false"
METRICS_ENDPOINT = f"{BASE_URL}/analytics/project-metrics?include_all=false"

DB_CONFIG = {
    "dbname": os.getenv('DB_NAME'),
    "user": os.getenv('DB_USER'),
    "password": os.getenv('DB_PASSWORD'),
    "host": os.getenv('DB_HOST'),
    "port": os.getenv('DB_PORT')
}

SCHEMA = os.getenv('DB_SCHEMA')

EMAIL_CONFIG = {
    "smtp_server": os.getenv('SMTP_SERVER'),
    "smtp_port": int(os.getenv('SMTP_PORT')),
    "sender_email": os.getenv('SENDER_EMAIL'),
    "sender_password": os.getenv('SENDER_PASSWORD'),
    "recipient_email": os.getenv('RECIPIENT_EMAIL')
}

# Set up logging
log_file = 'metrics_updater.log'
log_handler = RotatingFileHandler(log_file, maxBytes=1024*1024, backupCount=5)
logging.basicConfig(handlers=[log_handler], level=logging.ERROR, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

def generate_hash(api_key, secret_key, timestamp):
    """Generate authentication hash for API requests"""
    message = f"{api_key}{secret_key}{timestamp}"
    return hashlib.sha256(message.encode()).hexdigest()

def decrypt_field(encrypted_value):
    """Decrypt individual field values from API response"""
    try:
        encrypted_data = base64.b64decode(encrypted_value)
        cipher = AES.new(ENCRYPTION_KEY, AES.MODE_CBC, IV)
        decrypted_data = unpad(cipher.decrypt(encrypted_data), AES.block_size)
        return decrypted_data.decode('utf-8')
    except Exception as e:
        logging.error(f"Decryption failed for value: {encrypted_value}. Error: {str(e)}")
        return encrypted_value

def send_error_email(subject, body):
    """Send email notification for script errors"""
    msg = EmailMessage()
    msg.set_content(body)
    msg['Subject'] = subject
    msg['From'] = EMAIL_CONFIG['sender_email']
    msg['To'] = EMAIL_CONFIG['recipient_email']

    try:
        with smtplib.SMTP(EMAIL_CONFIG['smtp_server'], EMAIL_CONFIG['smtp_port']) as server:
            server.starttls()
            server.login(EMAIL_CONFIG['sender_email'], EMAIL_CONFIG['sender_password'])
            server.send_message(msg)
        logging.error("Error email sent successfully")
    except Exception as e:
        logging.error(f"Failed to send error email: {str(e)}")

def make_api_request(endpoint, params=None):
    """Make authenticated API request and handle decryption"""
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
        error_msg = f"API request failed: {str(e)}"
        logging.error(error_msg)
        send_error_email("API Request Failed", error_msg)
        return None

def create_table_if_not_exists(cur, schema_name, table_name, data):
    """Create or verify existence of required database tables"""
    columns = data[0].keys()
    column_defs = [f"{col} TEXT" for col in columns]
    
    if table_name == 'team_composition':
        column_defs.append("PRIMARY KEY (team_member_id)")
    elif table_name == 'project_metrics':
        column_defs.append("UNIQUE (project_id, metric_period)")
    
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        {', '.join(column_defs)}
    )
    """
    cur.execute(create_table_query)

def insert_data_to_db(schema_name, table_name, data):
    """Insert or update data in database with proper conflict handling"""
    data = data.get('data', [])

    if not data:
        print("No data to insert.")
        return "No data to insert."

    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                create_table_if_not_exists(cur, schema_name, table_name, data)
                    
                if table_name == 'team_composition':
                    cur.execute(f"TRUNCATE TABLE {schema_name}.{table_name}")  # Full refresh for team data
                    
                columns = data[0].keys()

                if table_name == 'project_metrics':
                    query = f"""
                    INSERT INTO {schema_name}.{table_name} ({','.join(columns)}) 
                    VALUES %s 
                    ON CONFLICT (project_id, metric_period) DO UPDATE 
                    SET {', '.join(f"{col} = EXCLUDED.{col}" for col in columns if col not in ['project_id', 'metric_period'])}
                    """
                else:
                    query = f"INSERT INTO {schema_name}.{table_name} ({','.join(columns)}) VALUES %s"
                    
                values = [[row[col] for col in columns] for row in data]
                execute_values(cur, query, values)

            conn.commit()
            print(f"Data inserted successfully into {table_name}.")
            return "Data insertion successful."

    except Exception as e:
        error_msg = f"Database insertion failed: {str(e)}"
        logging.error(error_msg)
        send_error_email("Database Insertion Failed", error_msg)

def main():
    """Update project metrics and team composition data"""
    try:
        # Update team composition data
        print("Fetching team composition updates...")
        team_data = make_api_request(TEAM_ENDPOINT)
        if team_data:
            insert_data_to_db(SCHEMA, 'team_composition', team_data)
        else:
            print("No team composition updates available")

        # Update project metrics
        print("Fetching project metrics updates...")
        metrics_data = make_api_request(METRICS_ENDPOINT)
        if metrics_data:
            insert_data_to_db(SCHEMA, 'project_metrics', metrics_data)
        else:
            print("No new project metrics available")

    except Exception as e:
        error_msg = f"Script execution failed: {str(e)}"
        logging.error(error_msg)
        send_error_email("Script Execution Failed", error_msg)
        return error_msg

if __name__ == "__main__":
    main()