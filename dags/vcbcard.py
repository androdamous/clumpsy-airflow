from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from datetime import datetime

# Define the dataset that will be produced
qualified_names = [
    'cred.table@vcbcard',
    'debit.table@vcbcard',
    'loan.table@vcbcard',
    'saving.table@vcbcard']

with DAG(
    dag_id='dataset_producer',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dataset'],
) as dag:
    
    @task(outlets=lambda qualified_name: [Dataset(qualified_name, extra={"partition": datetime.strftime(datetime.now(), "%Y-%m-%d")})])
    def fetch_data(qualified_name: str) :
        """Task that fetches data from an external source"""
        print("Fetching data from external source")
        return qualified_name
    
    fetch_vcbcard = fetch_data.expand(qualified_name=qualified_names)
    