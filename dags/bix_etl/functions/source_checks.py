from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.hooks.http import HttpHook

def check_sales_db_availability():
    """
        Checks if db returns data from public.vendas with no errors.
    """

    sales_db_hook = PostgresHook(postgres_conn_id='bix_vendas',schema='postgres')

    query = "SELECT 1 FROM public.venda limit 1;"
    try:
        sales_db_hook.get_records(sql=query)
    except:
        raise Exception('CONEXAO COM O BANCO DE DADOS VENDAS MAL SUCEDIDA')

def check_employees_api_availability():
    """
        Checks if api returns data from id endpoints with no errors.
    """
    httphook = HttpHook(http_conn_id="bix_api",method='GET')
    try:
        httphook.run(endpoint='api_challenge_junior/',data={'id':1},extra_options={'check_response': True})
    except AirflowException:
        raise Exception('CONEXAO COM A API EMPLOYEES MAL SUCEDIDA')


def check_categories_parquet_api_availability():
    """
        Checks if parquet file endpoint returns data with no errors.
    """
    httphook = HttpHook(http_conn_id="bix_parquet",method='GET')
    try:
        httphook.run(endpoint='challenge_junior/categoria.parquet',extra_options={'check_response': True})
    except AirflowException:
        raise Exception('CONEXAO COM O PARQUET FILE MAL SUCEDIDA')
