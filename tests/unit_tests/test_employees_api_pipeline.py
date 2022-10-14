import pytest
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.errors import UndefinedTable

class TestApiPipeline():

    def test_get_api_task_returns_data_as_expected(self,bix_dag):
        """
            Checks if fetched data has the right format
        """

        operator = bix_dag.get_task('employees_pipeline.get_api_employees_data_to_db')
        get_api_data = operator.python_callable
        employees_data  = get_api_data(testing=True)
        assert isinstance(employees_data,list) # assert function returns a list

        for el in employees_data:
            assert isinstance(el,tuple) # assert every element is a tuple
            assert isinstance(el[0],int) # assert first element is an id (int)
            assert isinstance(el[1],str) # assert second elemente is a name (string)

    def test_create_or_truncate_employees_table(self,bix_dag):
        """
            Checks if task successfully creates (or truncates) table
        """
        output_postgres_hook = PostgresHook(postgres_conn_id='bix_output_db',schema='dw_vendas')
        output_postgres_hook.run(sql="DROP TABLE IF EXISTS dim_funcionarios CASCADE;")

        query = """SELECT 1 FROM dim_funcionarios LIMIT 1"""

        with pytest.raises(UndefinedTable) as assert_table_was_dropped:
            output_postgres_hook.get_records(sql=query) # assert table was dropped

        operator = bix_dag.get_task('employees_pipeline.create_or_truncate_dim_funcionarios')
        # execute operator
        operator.execute(dict()) # dict() is for an empty context

        assert len(output_postgres_hook.get_records(sql=query)) == 0 # assert table exists and is empty
