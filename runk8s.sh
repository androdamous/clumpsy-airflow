helm install airflow apache-airflow/airflow -f values.yaml --namespace airflow --create-namespace

# kubectl port-forward deployment/airflow-webserver 8080:8080 -n airflow


# # Add the Helm repository
# helm repo add spark-operator https://kubeflow.github.io/spark-operator
# helm repo update

# # Install the operator into the spark-operator namespace and wait for deployments to be ready
# helm install spark-operator spark-operator/spark-operator \
#     --namespace spark-operator --create-namespace --wait