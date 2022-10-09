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

# def check_tables():
#
#     query = """ SELECT COUNT(table_name) FROM
#                 	information_schema.tables
#                 WHERE
#                     table_schema LIKE 'public%'
#                  	AND
#                 	table_name in ('dim_categorias','dim_funcionarios','fato_venda');
#     """
#
#     postgres_hook = PostgresHook(postgres_conn_id='bix_output_db',schema='dw_vendas')
#     tables_count = postgres_hook.get_pandas_df(sql=query)['count'].item()
#
#     if tables_count < 3:



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

    # 
    # check_if_tables_exist = PythonOperator(
    #     task_id = 'check_if_tables_exist',
    #     python_callable = check_tables
    #
    # )


    # with TaskGroup(group_id='check_data_sources_availability') as sources_check:



    with TaskGroup(group_id='employees_pipeline') as employees_pipeline:

        create_employees_table = PostgresOperator(
            task_id = 'create_employees_table',
            postgres_conn_id='bix_output_db',
            sql='create_dim_funcionarios.sql'
        )

        get_api_employees_data = PythonOperator(
            task_id = 'get_api_employees_data',
            python_callable  = get_api_data
        )

        create_employees_table >> get_api_employees_data


    with TaskGroup(group_id='categories_pipeline') as categories_pipeline:
        create_categories_table = PostgresOperator(
            task_id = 'create_categories_table',
            postgres_conn_id='bix_output_db',
            sql='create_dim_categorias.sql'
        )

        get_parquet_categories_data = PythonOperator(
            task_id = 'get_parquet_data',
            python_callable  = get_parquet_data
        )

        create_categories_table >> get_parquet_categories_data


    get_sales_data_to_db = GenericTransfer(
        task_id='get_sales_data_to_db',
        sql = 'get_source_sales_data.sql',
        destination_table = 'fato_venda',
        source_conn_id = 'bix_vendas',
        destination_conn_id = 'bix_output_db',
        preoperator = 'create_fato_vendas.sql'
    )

    create_sales_view = PostgresOperator(
            task_id = 'create_sales_view',
            postgres_conn_id='bix_output_db',
            sql='create_sales_view.sql'
    )


    start >> [employees_pipeline,categories_pipeline] >> get_sales_data_to_db >> create_sales_view

    # start >> [check_if_tables_exist,check_data_sources_availability] >> truncate_tables  >> [employees_pipeline,categories_pipeline] >> get_sales_data_to_db >> create_sales_view
