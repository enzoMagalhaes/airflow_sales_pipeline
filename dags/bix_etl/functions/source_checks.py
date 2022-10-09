from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.http_hook import HttpHook

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
