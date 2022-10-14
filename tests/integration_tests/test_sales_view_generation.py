from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from pendulum import timezone
from pandas import DataFrame

import time
class TestSalesViewGeneration:

    dag_id='bix_etl'
    hook = PostgresHook(postgres_conn_id='bix_output_db',schema='dw_vendas')
    test_execution_date= datetime(2022,10,2,0,tzinfo=timezone("America/Sao_Paulo"))

    def test_sales_view_is_generated_as_expected(self,bix_dag):
        """
            checks if view is generated after whole dag run and
            columns have the expected names and data types
        """
        # drop view if it exists
        self.hook.run(sql="DROP VIEW IF EXISTS vendas;")

        # execute whole dag
        bix_dag.run(start_date=self.test_execution_date,
                    end_date=self.test_execution_date,
                    run_backwards=False)

        # get a sample of the generated view
        view_sample = self.hook.get_pandas_df(sql="SELECT * FROM vendas limit 1")

        # assert output is a table
        assert view_sample is not None
        assert isinstance(view_sample,DataFrame)

        expected_columns = {'data_venda': 'object',
                            'venda':'int',
                            'nome_categoria':'object',
                            'nome_funcionario':'object'}

        # assert columns have the expected_names and data types
        for expected_name,expected_type in expected_columns.items():
            assert view_sample[expected_name] is not None
            assert view_sample[expected_name].dtype == expected_type

    def check_if_sales_view_has_the_expected_lines(self):
        """
            gets the count of each table and assert that they have the same count
        """
        view_lines_count = self.hook.get_records(sql="SELECT COUNT(*) FROM vendas")[0][0]
        fact_table_lines_count = self.hook.get_records(sql="SELECT COUNT(*) FROM fato_vendas")[0][0]

        assert isinstance(view_lines_count,int)
        assert view_lines_count == fact_table_lines_count
