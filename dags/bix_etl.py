from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
# from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow.hooks.http_hook import HttpHook

from datetime import datetime,timedelta

import pandas as pd
import os
import json

default_args = {

    'owner': 'bix_owner',
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

def save_data(df,filename:str):
    outdir = '/opt/airflow/dags/data'
    if not os.path.exists(outdir):
        os.mkdir(outdir)
    df.to_csv(f'{outdir}/{filename}',index=False)

def get_data(filename):
    return pd.read_csv(f'/opt/airflow/dags/data/{filename}')

def get_sales_data():
    query = """SELECT * FROM public.venda"""
    postgres_hook = PostgresHook(postgres_conn_id='bix_vendas',schema='postgres')
    sales = postgres_hook.get_pandas_df(sql=query)
    save_data(sales,'vendas.csv')

def insert_data_to_db():

    vendas  = get_data('vendas.csv')

    vendas = vendas.to_records(column_dtypes={
            'id_venda':'int32',
            'id_funcionario':'int',
            'id_categoria': 'int',
            'data_venda': 'str',
            'venda': 'int'
    })

    print(type(vendas[0][0]))

    # funcionarios = get_data('funcionarios.csv')
    # funcionarios = funcionarios.to_records()
    # print(funcionarios)
    #
    #
    # postgres_hook = PostgresHook(postgres_conn_id='bix_output_db',schema='dw_vendas')
    #
    # sales = postgres_hook.insert_rows('dim_funcionarios',funcionarios)
    # # sales = postgres_hook.insert_rows('fato_venda',vendas)

def get_api_data():
    httphook = HttpHook(http_conn_id="bix_api",method='GET')
    endpoint = 'api_challenge_junior/'

    employees = {'id_funcionario':[],'funcionario':[]}
    for i in range(1,10):
        curr_id = {'id':i}
        response = httphook.run(endpoint=endpoint,data=curr_id)
        employees['id_funcionario'].append(i)
        employees['funcionario'].append(response.text)

    employees = pd.DataFrame.from_dict(employees)
    save_data(employees,'funcionarios.csv')

def get_parquet_data():
    httphook = HttpHook(http_conn_id="bix_parquet",method='GET')
    endpoint = 'challenge_junior/categoria.parquet'
    response = httphook.run(endpoint=endpoint)

    outdir = '/opt/airflow/dags/data'
    with open(f'{outdir}/categorias.parquet','wb') as file:
        file.write(response.content)

create_table_query = """

            CREATE TABLE IF NOT EXISTS dim_funcionarios(

               id_funcionario INT PRIMARY KEY,
               nome_funcionario VARCHAR(80) NOT NULL

            );

            CREATE TABLE IF NOT EXISTS dim_categorias(

               id_categoria INT PRIMARY KEY,
               nome_categoria VARCHAR(80) NOT NULL

            );

            CREATE TABLE IF NOT EXISTS fato_venda(

               id_venda INT PRIMARY KEY,
               id_funcionario INT NOT NULL,
               id_categoria INT NOT NULL,
               data_venda DATE NOT NULL,
               venda INT NOT NULL,

               FOREIGN KEY (id_funcionario)
                    REFERENCES dim_funcionarios (id_funcionario),

               FOREIGN KEY (id_categoria)
                    REFERENCES dim_categorias (id_categoria)
            );
"""

with DAG(
    dag_id = 'bix_etl',
    default_args = default_args,
    description = 'bix_etl',
    start_date = datetime(2021,10,8,8),
    schedule_interval='@daily',
    catchup=False
) as dag:

    start = DummyOperator(task_id='start_task')

    get_sales_data_to_csv = PythonOperator(
        task_id = 'get_sales_data',
        python_callable  = get_sales_data
    )

    get_api_employees_data = PythonOperator(
        task_id = 'get_api_data',
        python_callable  = get_api_data
    )

    get_parquet_categories_data = PythonOperator(
        task_id = 'get_parquet_data',
        python_callable  = get_parquet_data
    )

    setup_postgres_tables = PostgresOperator(
        task_id = 'create_postgres_tables',
        postgres_conn_id='bix_output_db',
        sql=create_table_query
    )

    insert_to_db = PythonOperator(
        task_id = 'insert_data_to_db',
        python_callable  = insert_data_to_db
    )


    start >> [get_sales_data_to_csv,get_api_employees_data,get_parquet_categories_data] >> setup_postgres_tables
