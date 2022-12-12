from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                table="",
                insert_sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.insert_sql = insert_sql

    def execute(self, context):
        self.log.info(f"Start loading data into {self.table}")
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        fact_insert_sql = f"""
            INSERT INTO {self.table} 
            {self.insert_sql};
            """
        redshift.run(fact_insert_sql)

        self.log.info(f"DONE INSERTING DATA INTO {self.table}")
