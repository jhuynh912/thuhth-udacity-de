from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 tables_list = [],
                 test_query = "",
                 fail_output = "",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables_list = tables_list
        self.test_query = test_query
        self.fail_output = fail_output

    def execute(self, context):
        
        redshift_hook = PostgresHook("redshift")
        self.log.info("DONE GETTING CREDENTIALS")

        for table in self.tables_list:
            self.log.info(f"Running test for {table}...")
            records = redshift_hook.get_records(self.test_query + f"FROM {table}")

            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")

            num_records = records[0][0]
            if num_records == self.fail_output:
                raise ValueError(f"Data quality check failed. {table} contained failed output {num_records}")
            else:
                self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")





