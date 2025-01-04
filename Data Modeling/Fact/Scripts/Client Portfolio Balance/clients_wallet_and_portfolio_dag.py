from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from email_util import failure_email

PATH_TO_SCRIPT_FOLDER = "/usr/local/airflow/include/scripts"

default_args = {
    'owner': 'Oluwatomisin Soetan',
    'retries': 3,  # Adjust as needed
    'retry_delay': timedelta(minutes=2),  # Fixed typo
    'on_failure_callback': failure_email,
}

with DAG(
    dag_id='clients_wallet_and_portfolio',
    default_args=default_args,
    description='Exchange Clients Daily Wallet Balance',
    start_date=datetime(2023, 5, 7),
    catchup=False,
    schedule_interval='00 02 * * *',  # Runs at 2:00 AM UTC (12:00 AM WAT)
) as dag:

    task1 = BashOperator(
        task_id='exchange_client_portfolio',
        bash_command=f'python3 {PATH_TO_SCRIPT_FOLDER}/clientportfolio_balance.py',
        cwd=PATH_TO_SCRIPT_FOLDER
    )

    task1
