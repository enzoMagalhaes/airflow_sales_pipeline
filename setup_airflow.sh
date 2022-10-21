echo -e "AIRFLOW_UID=$(id -u)" > .env
docker-compose -f docker-compose-prod.yml up airflow-init
docker-compose -f docker-compose-prod.yml up
