"""
DAG for running Great Expectations validations on trade data
"""

from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from gxoperator import GXOperator
from creds import db_conn

# Define great expectations operator
gx = GXOperator()
source_engine, conn_source = db_conn()


# Define your Python function to run
def fact_trades():    
    json_result  = gx.run_expectations(data_asset_name='fact_trading_transactions',
                        expectation_suite_name='02_fact_trades_expectations')
    
    gx.expectations_to_db(checkpoint_jsonresult=json_result,source_engine=source_engine)
        
# Define your DAG
dag = DAG(
    'gx_trade_dag',
    description='Run a Python script in a DAG',
    start_date=datetime(2023, 10, 11),  # Specify the start date
    schedule_interval='0 0 * * *',
    tags =  ['great_expectations','public']
)

# Create a PythonOperator that runs your Python function
fact_trades_script = PythonOperator(
    task_id='02_fact_trades_expectations',
    python_callable=fact_trades,
    dag=dag,
)

# Set up the task dependency, if necessary
fact_trades_script