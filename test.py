from datetime import datetime
from pathlib import Path
from airflow import DAG
from airflow.utils import yaml
from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.operators.custom_object_launcher import CustomObjectLauncher


def _load_body_to_dict(body):
    try:
        body_dict = yaml.safe_load(body)
    except yaml.YAMLError as e:
        raise AirflowException(
            f"Exception when loading resource definition: {e}\n")
    return body_dict

def manage_template_specs(application_file):
    if application_file:
        try:
            filepath = Path(application_file.rstrip()).resolve(strict=True)
            print(filepath)
        except (FileNotFoundError, OSError, RuntimeError, ValueError):
            application_file_body = application_file
        else:
            application_file_body = filepath.read_text()
        template_body = _load_body_to_dict(application_file_body)
        print(filepath, application_file_body, template_body)

        # print(application_file_body)
        if not isinstance(template_body, dict):
            msg = f"application_file body can't transformed into the dictionary:\n{application_file_body}"
            raise TypeError(msg)
    return template_body


# print(x["metadata"])
# x = manage_template_specs(application_file)

submit_pyspark = SparkKubernetesOperator(
    task_id="submit_spark_pi",
    namespace="default",
    # path mount trong Airflow container
    application_file="./dags/task.template",
    do_xcom_push=False,
)


# launcher = CustomObjectLauncher(
#     name=submit_pyspark.name,
#     namespace=submit_pyspark.namespace,
#     kube_client=submit_pyspark.client,
#     custom_obj_api=submit_pyspark.custom_obj_api,
#     template_body=submit_pyspark.template_body,
# )

x = manage_template_specs("./dags/task.template")
# print(x)
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
        # path mount trong Airflow container
        application_file="./dags/task.template",
        do_xcom_push=False,
    )
    submit_pyspark
# print(launcher.body)
# print(submit_pyspark.template_body)