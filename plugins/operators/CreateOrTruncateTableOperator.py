# from airflow.providers.postgres.operators.postgres import PostgresOperator
# from airflow.providers.postgres.hooks.postgres import PostgresHook
#
#
# class CreateOrTruncateTableOperator(PostgresOperator):
#
#     def __init__(
#         self,
#         *,
#         sql: str | Iterable[str],
#         postgres_conn_id: str = 'postgres_default',
#         autocommit: bool = False,
#         parameters: Iterable | Mapping | None = None,
#         database: str | None = None,
#         runtime_parameters: Mapping | None = None,
#         **kwargs,
#     ) -> None:
#         super().__init__(**kwargs)
#         self.sql = sql
#         self.postgres_conn_id = postgres_conn_id
#         self.autocommit = autocommit
#         self.parameters = parameters
#         self.database = database
#         self.runtime_parameters = runtime_parameters
#         self.hook: PostgresHook | None = None
#
#
#
#
#     def execute(self,context:Context):
#
#
#
#
#
#     # @apply_defaults
#     # def __init__(self,
#     #              my_field,
#     #              *args,
#     #              **kwargs):
#     #     super(MyOperator, self).__init__(*args, **kwargs)
#     #     self.my_field = my_field
#     #
#     # def execute(self, context):
#     #     hook = MyHook('my_conn')
#     #     hook.my_method(
