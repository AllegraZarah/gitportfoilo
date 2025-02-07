# %%
# pip install requests pycryptodome psycopg2-binary python-dotenv

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

# %%
# Configuration
API_KEY = os.getenv('YOUR_API_KEY')
SECRET_KEY = os.getenv('YOUR_SECRET_KEY')
ENCRYPTION_KEY = os.getenv('ENCRYPTION_KEY').encode()
IV = os.getenv('IV').encode()

BASE_URL = "https://your-api-url.com/api"
EMPLOYEE_ENDPOINT = f"{BASE_URL}/partners/all-staff?is_paginated=false"
APPRAISAL_ENDPOINT = f"{BASE_URL}/partners/appraisals?is_paginated=false"

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

# %%
# Set up logging
log_file = 'api_script.log'
log_handler = RotatingFileHandler(log_file, maxBytes=1024*1024, backupCount=5)
logging.basicConfig(handlers=[log_handler], level=logging.ERROR, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# %%
def generate_hash(api_key, secret_key, timestamp):
    message = f"{api_key}{secret_key}{timestamp}"
    return hashlib.sha256(message.encode()).hexdigest()

# %%
def decrypt_field(encrypted_value):
    try:
        # Decode the base64 encoded encrypted value
        encrypted_data = base64.b64decode(encrypted_value)
            
        # Create a new AES cipher object for each field
        cipher = AES.new(ENCRYPTION_KEY, AES.MODE_CBC, IV)
            
        # Decrypt and unpad
        decrypted_data = unpad(cipher.decrypt(encrypted_data), AES.block_size)
            
        # Return the decoded string
        return decrypted_data.decode('utf-8')
    

    except Exception as e:
        logging.error(f"Decryption failed for value: {encrypted_value}. Error: {str(e)}")
        return encrypted_value  # Return the original value if decryption fails

# %%
def send_error_email(subject, body):
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

# %%
def make_api_request(endpoint, params=None):
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
        decrypted_data = encrypted_data.copy()  # Create a copy to store decrypted data

        # Decrypt each field in the results
        if 'data' in decrypted_data and isinstance(decrypted_data['data'], list):
            print("'results' list found, list iteration will commence")  # Debugging

            for item in decrypted_data['data']:
                for key, value in item.items():
                    if isinstance(value, str):
                        decrypted_value = decrypt_field(value)
                        item[key] = decrypted_value
            print("'results' list Decrypted")
            result = decrypted_data

        else:
            print("List iteration never occurred")  # Debugging
            result = None

        return result  # Return the decrypted data or None

    except requests.RequestException as e:
        error_msg = f"API request failed: {str(e)}"
        logging.error(error_msg)
        send_error_email("API Request Failed", error_msg)
        return None

# %%
def create_table_if_not_exists(cur, schema_name, table_name, data):
    columns = data[0].keys()
    column_defs = [f"{col} TEXT" for col in columns]
    
    if table_name == 'example_employee':
        column_defs.append("PRIMARY KEY (employee_id)")
    elif table_name == 'example_appraisal':
        column_defs.append("UNIQUE (employee_code, appraisal_cycle)")
    
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        {', '.join(column_defs)}
    )
    """
    cur.execute(create_table_query)

# %%
def insert_data_to_db(schema_name, table_name, data):
    data = data.get('data', [])

    if not data:
        print("No data to insert.")
        return "No data to insert."

    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                create_table_if_not_exists(cur, schema_name, table_name, data)
                    
                if table_name == 'example_employee':
                    cur.execute(f"TRUNCATE TABLE {schema_name}.{table_name}")  # Full refresh
                    
                columns = data[0].keys()

                if table_name == 'example_appraisal':
                    query = f"""
                    INSERT INTO {schema_name}.{table_name} ({','.join(columns)}) 
                    VALUES %s 
                    ON CONFLICT (id) DO UPDATE 
                    SET {', '.join(f"{col} = EXCLUDED.{col}" for col in columns if col != 'id')}
                    """
                else:
                    query = f"INSERT INTO {schema_name}.{table_name} ({','.join(columns)}) VALUES %s"
                    
                values = [[row[col] for col in columns] for row in data]
                print(f"Executing query: {query}")
                execute_values(cur, query, values)

            conn.commit()
            print("Data inserted successfully.")
            return "Data insertion successful."

    except Exception as e:
        error_msg = f"Database insertion failed: {str(e)}"
        logging.error(error_msg)
        send_error_email("Database Insertion Failed", error_msg)

# %%
def main():
    try:
        # Fetch and update employee data
        print("Fetching employee data...")
        employee_data = make_api_request(EMPLOYEE_ENDPOINT)
        if employee_data:
            print(f"Employee data fetched")
            insert_data_to_db(SCHEMA, 'example_employee', employee_data)
            print("Employee data inserted into database.")

        else:
            print("No Employee data Inserted")


        # Fetch and update appraisal data
        print("Fetching appraisal data...")
        appraisal_data = make_api_request(APPRAISAL_ENDPOINT)
        if appraisal_data:
            print(f"Appraisal data fetched")
            insert_data_to_db(SCHEMA, 'example_appraisal', appraisal_data)
            print("Appraisal data inserted into database.")

        else:
            print("No Appraisal data Inserted")
            

    except Exception as e:
        error_msg = f"Script execution failed: {str(e)}"
        logging.error(error_msg)
        send_error_email("Script Execution Failed", error_msg)
        return error_msg

if __name__ == "__main__":
    main()
