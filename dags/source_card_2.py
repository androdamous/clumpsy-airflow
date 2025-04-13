from airflow import DAG, Dataset
# from airflow.datasets import DatasetAlias
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from datetime import datetime

# Define the dataset that will be produced
qualified_names = [
    'cred.table@vcbcard',
    'debit.table@vcbcard',
    'loan.table@vcbcard',
    'saving.table@vcbcard']
datasets = [Dataset(qualified_name) for qualified_name in qualified_names]
alias = Dataset("fetch_etl")

with DAG(
    dag_id='dataset_producer_2',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dataset'],
) as dag:
    
    alias = Dataset("fetch_etl", extra={"id": ["debit.table@vcbcard"]})

    @task(outlets=[alias])
    def fetch_data(*, outlet_events: str):
        """Task that fetches data from an external source"""
        print("Fetching data from external source")
        outlet_events[alias].add(datasets[1])
        return 1