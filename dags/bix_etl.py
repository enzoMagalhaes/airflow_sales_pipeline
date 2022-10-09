from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.generic_transfer import GenericTransfer
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.http_hook import HttpHook
from datetime import datetime,timedelta

default_args = {

    'owner': 'bix_owner',
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

def get_api_data():
    httphook = HttpHook(http_conn_id="bix_api",method='GET')
    endpoint = 'api_challenge_junior/'

    employees = []
    for i in range(1,10):
        curr_id = {'id':i}
        response = httphook.run(endpoint=endpoint,data=curr_id)
        employees.append((i,response.text))

    output_postgres_hook = PostgresHook(postgres_conn_id='bix_output_db',schema='dw_vendas')
    output_postgres_hook.insert_rows('dim_funcionarios',employees)

def get_parquet_data():
    from pandas import read_parquet

    httphook = HttpHook(http_conn_id="bix_parquet",method='GET')
    _ = httphook.get_conn() #initialize connection inside class
    url = httphook.url_from_endpoint(endpoint='challenge_junior/categoria.parquet')
    parquet_dataframe = read_parquet(url)
    parquet_dataframe.columns = ['id_categoria','nome_categoria']

    output_postgres_hook = PostgresHook(postgres_conn_id='bix_output_db',schema='dw_vendas')
    parquet_dataframe.to_sql(name = 'dim_categorias',
                             con=output_postgres_hook.get_sqlalchemy_engine(),
                             if_exists='append',
                             index=False)


def check_sales_db_availability():
    sales_db_hook = PostgresHook(postgres_conn_id='bix_vendas',schema='postgres')

    query = """SELECT 1 FROM public.venda limit 1;"""
    if not sales_db_hook.get_records(sql=query):
        raise ValueError('CONEXAO COM O BANCO DE DADOS SALES MAL SUCEDIDA')

def check_employees_api_availability():
    httphook = HttpHook(http_conn_id="bix_api",method='GET')
    httphook.run(endpoint='api_challenge_junior/',data={'id':1},extra_options={'check_response': True})

def check_categories_parquet_api_availability():
    httphook = HttpHook(http_conn_id="bix_parquet",method='GET')
    httphook.run(endpoint='challenge_junior/categoria.parquet',extra_options={'check_response': True})



with DAG(
    dag_id = 'bix_etl',
    default_args = default_args,
    description = 'bix_etl',
    start_date = datetime(2021,10,8,8),
    schedule_interval='@daily',
    template_searchpath='/opt/airflow/include/sql',
    catchup=False
) as dag:

    start = DummyOperator(task_id='start_task')

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
            sql='create_or_truncate_dim_funcionarios.sql'
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
            sql='create_or_truncate_dim_categorias.sql'
        )


        get_parquet_categories_data = PythonOperator(
            task_id = 'get_parquet_data_to_db',
            python_callable  = get_parquet_data
        )

        create_or_truncate_categories_table >> get_parquet_categories_data

    get_sales_data_to_db = GenericTransfer(
        task_id='get_sales_data_to_db',
        sql = 'get_source_sales_data.sql',
        destination_table = 'fato_vendas',
        source_conn_id = 'bix_vendas',
        destination_conn_id = 'bix_output_db',
        preoperator = 'create_or_truncate_fato_vendas.sql'
    )

    create_sales_view = PostgresOperator(
            task_id = 'create_sales_view',
            postgres_conn_id='bix_output_db',
            sql='create_sales_view.sql'
    )

    start >> sources_check >> [employees_pipeline,categories_pipeline,get_sales_data_to_db] >> create_sales_view
