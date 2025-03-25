from airflow import DAG
from airflow.providers.apache.spark.operators.spark_kubernetes import SparkKubernetesOperator

from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
}

with DAG(
    dag_id='spark_k8s_example',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    spark_job = SparkKubernetesOperator(
        task_id='spark_k8s_task',
        namespace='default',  # the Kubernetes namespace where your SparkOperator is running
        application_file='D:\Works\clumpsy-airflow\kubedags\application.yaml',  # path to your SparkApplication definition file
        kubernetes_conn_id='kubernetes_default'  # ensure this connection exists in Airflow
    )
    
    spark_job
