from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from datetime import datetime

with DAG(
    dag_id="pyspark_pi_on_k8s",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["spark", "kubernetes"],
) as dag:

    submit_pyspark = SparkKubernetesOperator(
        task_id="submit_spark_pi",
        namespace="default",
        application_file="/opt/airflow/dags/task.yaml.j2",  # path mount trong Airflow container
        do_xcom_push=False,
    )