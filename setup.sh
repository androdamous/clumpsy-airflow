docker rm -f airflow

docker volume rm airflow_db

docker run -d --name airflow -p 8080:8080 \
  -e _AIRFLOW_WWW_USER_CREATE=true \
  -e _AIRFLOW_WWW_USER_USERNAME=admin \
  -e _AIRFLOW_WWW_USER_PASSWORD=admin \
  -e _AIRFLOW_WWW_USER_EMAIL=admin@example.com \
  -e _AIRFLOW_WWW_USER_FIRSTNAME=admin \
  -e _AIRFLOW_WWW_USER_LASTNAME=admin \
  -e KUBECONFIG=/opt/airflow/kubeconfig \
  -v ./dags:/opt/airflow/dags \
  -v ./logs:/opt/airflow/logs \
  -v ./plugins:/opt/airflow/plugins \
  apache/airflow:2.6.0 standalone

docker exec -it airflow airflow users create \
    --username admin1 \
    --firstname YourFirstName \
    --lastname YourLastName \
    --role Admin \
    --email your_email@example.com \
    --password admin1
