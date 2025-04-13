import pandas as pd
from airflow import DAG
from airflow.decorators import task, dag
import os
import random
from datetime import datetime

from context.manifest import Manifest, ManifestManager

@dag(
    dag_id='fetch_data',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['fetch'],
)
def fetch_data_dag():
    """
    DAG that generates sample data and creates a manifest file. 
    The sample data is generated in a folder structure based on the table name and date.
    """
    @task()
    def generate_sample_data():
        """Task that generates sample data"""
        # Generate random data
        data = {
            'column1': [random.randint(1, 100) for _ in range(10)],
            'column2': [random.choice(['A', 'B', 'C', 'D']) for _ in range(10)]
        }
        df = pd.DataFrame(data)

        # Generate folder path based on the current date
        table_name = "sample_table"
        date_str = datetime.now().strftime('%Y%m%d')
        folder_path = f'/opt/airflow/plugins/{table_name}/{date_str}'
        os.makedirs(folder_path, exist_ok=True)

        # Write data to CSV
        file_path = os.path.join(folder_path, 'data.csv')
        df.to_csv(file_path, index=False)
        return file_path
    
    @task()
    def generate_manifest(data_path: str):
        """Task that generates a manifest file"""
        # Generate manifest data
        manifest = Manifest(
            table_name="sample_table",
            table_qualified_name="sample_table@card",
            path=data_path,
            partition_date=datetime.now().strftime('%Y%m%d'),
            load_status="success"
        ) 
        # Write manifest to YAML
        if "/opt/airflow/plugins/manifests" not in os.listdir("/opt/airflow/plugins"):
            os.makedirs("/opt/airflow/plugins/manifests", exist_ok=True)
        manifest_mng = ManifestManager("/opt/airflow/plugins/manifests")
        manifest_file_path = manifest_mng.generate_manifest(manifest)
        return manifest_file_path
    
    file_path = generate_sample_data()
    generate_manifest(file_path)

fetch_data_dag()
