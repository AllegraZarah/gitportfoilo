from datetime import datetime,timedelta
import pendulum
from airflow import DAG
from airflow.operators.bash  import BashOperator
from email_util import failure_email

PATH_TO_DBT_VENV = "/usr/local/airflow/my_venv/bin/activate"
PATH_TO_DBT_PROJECT = "/usr/local/airflow/include/dbt/analytics_mart"
PATH_TO_DBT_PROFILE = "/usr/local/airflow/include/dbt/.dbt"

default_args  = {
     'owner' : 'Portfolio Owner',
     'retries' : 0,
     'retry_dalay' : timedelta(minutes=2),
     'on_failure_callback': failure_email
}
with DAG(
    dag_id = 'dbt_analytics_mart_dag',
    default_args = default_args,
    description =  'analytics_mart_dag',
    start_date= pendulum.datetime(2023, 10, 11, tz="Europe/Paris"),
    catchup=False,
    schedule_interval =   '00 00 * * *',
    tags =  ['dbt','analytics_mart']
) as dag:

    task0 =  BashOperator(
        task_id = 'dbt_deps',
        bash_command = 'source $PATH_TO_DBT_VENV  && export DBT_PROFILES_DIR=$PATH_TO_DBT_PROFILE  && dbt deps',
        cwd=PATH_TO_DBT_PROJECT,
        env={"PATH_TO_DBT_VENV": PATH_TO_DBT_VENV,"PATH_TO_DBT_PROFILE":PATH_TO_DBT_PROFILE},
    )

    task1 =  BashOperator(
        task_id = 'dbt_run',
        bash_command = 'source $PATH_TO_DBT_VENV  && export DBT_PROFILES_DIR=$PATH_TO_DBT_PROFILE  && dbt run', 
        cwd=PATH_TO_DBT_PROJECT,
        env={"PATH_TO_DBT_VENV": PATH_TO_DBT_VENV,"PATH_TO_DBT_PROFILE":PATH_TO_DBT_PROFILE},
    )

    task2 =  BashOperator(
        task_id = 'dbt_test',
        bash_command = 'source $PATH_TO_DBT_VENV  && export DBT_PROFILES_DIR=$PATH_TO_DBT_PROFILE  && dbt test || exit 0',
        cwd=PATH_TO_DBT_PROJECT,
        env={"PATH_TO_DBT_VENV": PATH_TO_DBT_VENV,"PATH_TO_DBT_PROFILE":PATH_TO_DBT_PROFILE},
    )

    task3 =  BashOperator(
        task_id = 'dbt_docs_generate',
        bash_command = 'source $PATH_TO_DBT_VENV  && export DBT_PROFILES_DIR=$PATH_TO_DBT_PROFILE  && dbt docs generate',
        cwd=PATH_TO_DBT_PROJECT,
        env={"PATH_TO_DBT_VENV": PATH_TO_DBT_VENV,"PATH_TO_DBT_PROFILE":PATH_TO_DBT_PROFILE},
    )


  
task0 >> task1 >> task2 >> task3
