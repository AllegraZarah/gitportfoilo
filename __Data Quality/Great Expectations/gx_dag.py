"""
DAG for running Great Expectations validations on transaction data
"""
from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator

# Configuration
POSTGRES_CONN_ID = "postgres_default"  # Update in Airflow connections
ROOT_DIR = "/path/to/great_expectations"  # Update with your GX installation path

default_args = {
    'owner': 'data_team',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

@dag(
    dag_id='transaction_data_validation',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule='0 0 * * *',  # Daily at midnight, modify as needed
    catchup=False,
    tags=['data_quality', 'great_expectations'],
    description='Validates transaction data using Great Expectations'
)
def validate_transaction_data():
    """
    DAG to validate transaction data tables using Great Expectations.
    Each task validates a different aspect of the transaction data.
    """
    
    # Validate main transactions table
    validate_transactions = GreatExpectationsOperator(
        task_id="validate_transactions",
        conn_id=POSTGRES_CONN_ID,
        data_context_root_dir=ROOT_DIR,
        data_asset_name="transactions",
        checkpoint_name="transactions_checkpoint",
        expectation_suite_name="transactions_expectations",
        return_json_dict=True,
        fail_task_on_validation_failure=True,
    )

    # Validate transaction logs table
    validate_transaction_logs = GreatExpectationsOperator(
        task_id="validate_transaction_logs",
        conn_id=POSTGRES_CONN_ID,
        data_context_root_dir=ROOT_DIR,
        data_asset_name="transaction_logs",
        checkpoint_name="transaction_logs_checkpoint",
        expectation_suite_name="transaction_logs_expectations",
        return_json_dict=True,
        fail_task_on_validation_failure=True,
    )

    # Define task dependencies
    validate_transactions >> validate_transaction_logs

# Initialize the DAG
dag = validate_transaction_data()