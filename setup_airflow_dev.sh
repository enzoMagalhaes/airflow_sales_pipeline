docker build . --tag airflow_2.3.0_with_pytest:latest
echo -e "AIRFLOW_UID=$(id -u)" > .env
docker-compose up airflow-init
docker-compose up
