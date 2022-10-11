import pytest
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pandas import DataFrame

from psycopg2.errors import UndefinedTable

class TestPostgresDBPipeline():

    def test_GenericTransfer_inputs_data_as_expected(self,bix_dag):

        # get lines count from input db
        input_postgres_hook = PostgresHook(postgres_conn_id='bix_vendas',schema='postgres')
        input_lines_count = input_postgres_hook.get_records(sql="SELECT COUNT(*) FROM public.venda;")[0][0]

        # drop table from output db
        output_postgres_hook = PostgresHook(postgres_conn_id='bix_output_db',schema='dw_vendas')
        output_postgres_hook.run(sql="DROP TABLE IF EXISTS fato_vendas CASCADE;")

        # assert table was dropped
        with pytest.raises(UndefinedTable) as assert_table_was_dropped:
            output_postgres_hook.get_records(sql="SELECT 1 FROM fato_vendas LIMIT 1")

        # execute GenericTransfer operator to feed data from input db to output db
        operator = bix_dag.get_task('get_sales_data_to_db')
        operator.execute(dict())

        # assert fato_vendas was created and the it has the same number of lines as the input db
        assert output_postgres_hook.get_records(sql="SELECT COUNT(*) FROM fato_vendas")[0][0] == input_lines_count

        sample_data_query = """SELECT
                                    id_venda,
                                    id_funcionario,
                                    id_categoria,
                                    data_venda,
                                    venda
                               FROM fato_vendas LIMIT 5
                            """
        sample_data = output_postgres_hook.get_pandas_df(sql=sample_data_query)

        expected_types = {
                'id_venda': 'int64',
                'id_funcionario':'int64',
                'id_categoria':'int64',
                'data_venda':'object',
                'venda':'int64'
        }

        # assert all the data types match the expected types
        for col in sample_data.columns:
            expected_type = expected_types[col]
            assert sample_data[col].dtype == expected_type
