from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.generic_transfer import GenericTransfer
from datetime import datetime,timedelta
from pendulum import timezone

from bix_etl.functions.helpers import get_api_data, get_parquet_data
from bix_etl.functions.source_checks import (check_sales_db_availability,
                                             check_employees_api_availability,
                                             check_categories_parquet_api_availability)

default_args = {
    'owner': 'bix_owner',
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id = 'bix_etl',
    default_args = default_args,
    description = 'populate the DW with the sales, employees and product categories data',
    start_date = datetime(2022,10,9,0,tzinfo=timezone("America/Sao_Paulo")),
    schedule_interval='0 0 * * *',
    template_searchpath='/opt/airflow/include',
    catchup=False
) as dag:

    start = EmptyOperator(task_id='start')

    with TaskGroup(group_id='check_data_sources_availability') as sources_check:

        check_sales_db = PythonOperator(
                task_id = 'check_sales_db_availability',
                python_callable  = check_sales_db_availability
            )

        check_employees_api = PythonOperator(
                task_id = 'check_employees_api_availability',
                python_callable = check_employees_api_availability
            )

        check_parquet_api_storage = PythonOperator(
                task_id = 'check_categories_parquet_api_availability',
                python_callable = check_categories_parquet_api_availability
            )

        [check_sales_db,check_employees_api,check_parquet_api_storage]



    with TaskGroup(group_id='employees_pipeline') as employees_pipeline:

        create_or_truncate_employees_table= PostgresOperator(
            task_id = 'create_or_truncate_dim_funcionarios',
            postgres_conn_id='bix_output_db',
            sql='sql/create_or_truncate_dim_funcionarios.sql'
        )

        get_api_employees_data = PythonOperator(
            task_id = 'get_api_employees_data_to_db',
            python_callable  = get_api_data
        )

        create_or_truncate_employees_table >> get_api_employees_data


    with TaskGroup(group_id='categories_pipeline') as categories_pipeline:

        create_or_truncate_categories_table= PostgresOperator(
            task_id = 'create_or_truncate_dim_categorias',
            postgres_conn_id='bix_output_db',
            sql='sql/create_or_truncate_dim_categorias.sql'
        )


        get_parquet_categories_data = PythonOperator(
            task_id = 'get_parquet_data_to_db',
            python_callable  = get_parquet_data
        )

        create_or_truncate_categories_table >> get_parquet_categories_data

    get_sales_data_to_db = GenericTransfer(
        task_id='get_sales_data_to_db',
        sql = 'sql/get_source_sales_data.sql',
        destination_table = 'fato_vendas',
        source_conn_id = 'bix_vendas',
        destination_conn_id = 'bix_output_db',
        preoperator = 'sql/create_or_truncate_fato_vendas.sql'
    )

    create_sales_view = PostgresOperator(
            task_id = 'create_sales_view',
            postgres_conn_id='bix_output_db',
            sql='sql/create_sales_view.sql'
    )

    start >> sources_check >> [employees_pipeline,categories_pipeline,get_sales_data_to_db] >> create_sales_view
