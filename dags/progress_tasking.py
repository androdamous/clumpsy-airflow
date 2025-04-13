# simple_dag.py
from datetime import datetime, timedelta
import time
from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from airflow.decorators import task

# Import task functions from separate files
from get_source_name import get_source_name
from get_etl import get_etl
from run_etl import run_etl
from update_etl import update_etl

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

qualified_names = [
    'cred.table@vcbcard',
    'debit.table@vcbcard',
    'loan.table@vcbcard',
    'saving.table@vcbcard']

datasets = [Dataset(qualified_name) for qualified_name in qualified_names]
# Instantiate the DAG
with DAG(
    dag_id='simple_dag',
    default_args=default_args,
    description='A simple DAG example that passes values between tasks',
    schedule=[Dataset("fetch_etl")],
    catchup=False,
    max_active_runs=3,
    concurrency=3,
    
) as dag:
    

    # Task 1: Get the source name using an initial input value
    task_get_source_name = PythonOperator(
        task_id='get_source_name',
        python_callable=get_source_name,
        op_kwargs={'input_value': 'Initial Input Value'},
        inlets=[Dataset("fetch_etl")],
    )

    # Task 2: Configure ETL based on the source name
    task_get_etl = PythonOperator(
        task_id='get_etl',
        python_callable=get_etl,
    )

    # Task 3: Run the ETL process using the configuration
    task_run_etl = PythonOperator(
        task_id='run_etl',
        python_callable=run_etl,
    )

    @task(max_active_tis_per_dag=3)
    def run_etl(x: int):
        time.sleep(5)
        return x + 1
    items = [1, 2, 3]
    task_run_etl_parallel = run_etl.expand(x=items)

    # Task 4: Update/finalize the ETL process
    task_update_etl = PythonOperator(
        task_id='update_etl',
        python_callable=update_etl,
    )

    

    # Set task dependencies so that each task passes its result to the next one
    task_get_source_name >> task_get_etl >> task_run_etl_parallel >> task_update_etl
