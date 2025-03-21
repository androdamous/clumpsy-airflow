docker rm -f airflow

docker volume rm airflow_db

docker run -d --name airflow -p 8080:8080 ^
  -e _AIRFLOW_WWW_USER_CREATE=true ^
  -e _AIRFLOW_WWW_USER_USERNAME=admin ^
  -e _AIRFLOW_WWW_USER_PASSWORD=admin ^
  -e _AIRFLOW_WWW_USER_EMAIL=admin@example.com ^
  -e _AIRFLOW_WWW_USER_FIRSTNAME=admin ^
  -e _AIRFLOW_WWW_USER_LASTNAME=admin ^
  -v "%cd%^dags:/opt/airflow/dags" ^
  -v "%cd%^logs:/opt/airflow/logs" ^
  -v "%cd%^plugins:/opt/airflow/plugins" ^
  apache/airflow:2.6.0 standalone

docker exec -it airflow airflow users create ^
    --username admin ^
    --firstname YourFirstName ^
    --lastname YourLastName ^
    --role Admin ^
    --email your_email@example.com ^
    --password admin
