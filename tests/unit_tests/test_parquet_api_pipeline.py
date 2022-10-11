import pytest
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pandas import DataFrame

from psycopg2.errors import UndefinedTable

class TestParquetAPIPipeline():

    def test_get_parquet_data_returns_data_as_expected(self,bix_dag):
        operator = bix_dag.get_task('categories_pipeline.get_parquet_data_to_db')
        get_parquet_data = operator.python_callable
        categories_data  = get_parquet_data(testing=True)

        assert isinstance(categories_data,DataFrame) # assert function returns a dataframe
        assert categories_data['id_categoria'].dtype == 'int64' # assert id is an integer
        assert categories_data['nome_categoria'].dtype == 'object' # assert name is a string (object)

    def test_create_or_truncate_categories_table(self,bix_dag):
        output_postgres_hook = PostgresHook(postgres_conn_id='bix_output_db',schema='dw_vendas')
        output_postgres_hook.run(sql="DROP TABLE IF EXISTS dim_categorias CASCADE;")

        query = """SELECT 1 FROM dim_categorias LIMIT 1"""

        with pytest.raises(UndefinedTable) as assert_table_was_dropped:
            output_postgres_hook.get_records(sql=query) # assert table was dropped

        operator = bix_dag.get_task('categories_pipeline.create_or_truncate_dim_categorias')
        # execute operator
        operator.execute(dict()) # dict() is for an empty context

        assert len(output_postgres_hook.get_records(sql=query)) == 0 # assert table exists and is empty
