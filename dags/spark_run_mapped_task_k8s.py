from airflow import DAG
from airflow.decorators import task
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.decorators import dag
from datetime import datetime
from kubernetes.client.exceptions import ApiException
from kubernetes import client, config
from airflow.utils.decorators import apply_defaults
import yaml
import time
 
class CustomSparkKubernetesOperator(SparkKubernetesOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        self.delay = kwargs.pop('delay', None)
        super().__init__(*args, **kwargs)

    def get_application_name_from_file(self):
        application_content = self.application_file.replace('\t', ' ' * 2)
        with open(application_content) as f:
            application_config = yaml.safe_load(f)
        return application_config['metadata']['name']

    def update_application_name(self):
        timestamp = datetime.now().strftime('%H%M')
        application_content = self.application_file.replace('\t', ' ' * 2)
        with open(application_content) as f:
            application_config = yaml.safe_load(f)
        application_name = application_config['metadata']['name']
        unique_application_name = f"{application_name}-{timestamp}"
        application_config['metadata']['name'] = unique_application_name
        self.application_file = yaml.dump(application_config)

    def delete_existing_application(self, name: str, namespace: str):
        """
        Deletes any existing SparkApplication with the specified name in the given namespace.
        """
        # Load Kubernetes configuration (inside the cluster or from kubeconfig file)
        config.load_kube_config()  # Use load_kube_config() if running locally for testing
 
        # Initialize Kubernetes API client
        api_instance = client.CustomObjectsApi()
        spark_apps = api_instance.list_namespaced_custom_object(
            group="sparkoperator.k8s.io",
            version="v1beta2",  # Version used by the Spark Operator; adjust if needed
            namespace=namespace,
            plural="sparkapplications"
        )
 
        try:
            # Attempt to delete the SparkApplication if it exists
            for app in spark_apps.get("items", []):
                app_name = app["metadata"]["name"]
                if app_name.startswith(name):
                    api_instance.delete_namespaced_custom_object(
                        group="sparkoperator.k8s.io",
                        version="v1beta2",  # Version used by the Spark Operator; adjust if needed
                        namespace=namespace,
                        plural="sparkapplications",
                        name=app_name
                    )
                    self.log.info(f"Deleted existing Spark application: {app_name}")
        except client.exceptions.ApiException as e:
            if e.status == 404:
                self.log.info(f"No existing Spark application found with name: {name}")
            else:
                raise  # Re-raise other exceptions
 
    def execute(self, context):
        try:
            if self.delay:
                self.log.info(f'Delaying execution for {self.delay} seconds ... ')
                time.sleep(self.delay)
            self.log.info(f'Starting spark job ...')
            # Run the original SparkKubernetesOperator's execute method
            task_instance_id = context['task_instance'].task_id
            self.delete_existing_application(name=self.get_application_name_from_file(), namespace=self.namespace)
            self.update_application_name()
            result = super().execute(context)
            return result
        except ApiException as e:
            # Catch the "Too Old Resource Version" error
            if "too old resource version" in str(e):
                self.log.info("Ignoring 'Too Old Resource Version' error and skipping retry.")
                return "Done"  # Return something to prevent retry
            else:
                # Re-raise other exceptions
                raise e
from airflow import DAG
from airflow.decorators import task
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from datetime import datetime

with DAG(
    dag_id="pyspark_pi_on_k8s_mapped",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["spark", "kubernetes", "mapped"],
) as dag:

    @task
    def list_spark_jobs():
        return [
            "/opt/airflow/dags/task.template",
            "/opt/airflow/dags/task.template",
            "/opt/airflow/dags/task.template"
        ]

    spark_files = list_spark_jobs()
    
    submit_jobs = CustomSparkKubernetesOperator.partial(
        task_id="submit_spark_job",  # Airflow will auto-append index like submit_spark_job__0
        namespace="default",
        do_xcom_push=False,
    ).expand(application_file=spark_files)