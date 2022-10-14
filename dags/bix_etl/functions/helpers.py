from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.hooks.http import HttpHook

def get_api_data(testing:bool = False):
    """
        loop through every id from 1 to 9 to get all the employees
        names from the api and insert the (ids,names) tuples into a
        list and insert the generated data to output db.
    """
    httphook = HttpHook(http_conn_id="bix_api",method='GET')
    endpoint = 'api_challenge_junior/'

    # loop through every id from 1 to 9 to get the employees names and insert them into the list
    employees = []
    for i in range(1,10):
        curr_id = {'id':i}
        response = httphook.run(endpoint=endpoint,data=curr_id)
        employees.append((i,response.text))

    # for testing purposes only, never use it in production!
    if testing:
        return employees

    # insert data to db
    output_postgres_hook = PostgresHook(postgres_conn_id='bix_output_db',schema='dw_vendas')
    output_postgres_hook.insert_rows('dim_funcionarios',employees)

def get_parquet_data(testing:bool = False):
    """
        load parquet file into a dataframe using pandas library,
        change the column names to match the db table names and
        insert data to db.
    """
    from pandas import read_parquet
    httphook = HttpHook(http_conn_id="bix_parquet",method='GET')
    _ = httphook.get_conn() #initialize connection inside class (look at httphook source code)

    # get the dataframe url and load the parquet file to a dataframe using pandas
    url = httphook.url_from_endpoint(endpoint='challenge_junior/categoria.parquet')
    parquet_dataframe = read_parquet(url)

    # change the column names to match the db table column names
    parquet_dataframe.columns = ['id_categoria','nome_categoria']

    # for testing purposes only, never use it in production!
    if testing:
        return parquet_dataframe

    # insert data to db
    output_postgres_hook = PostgresHook(postgres_conn_id='bix_output_db',schema='dw_vendas')
    parquet_dataframe.to_sql(name = 'dim_categorias',
                             con=output_postgres_hook.get_sqlalchemy_engine(),
                             if_exists='append',
                             index=False)
