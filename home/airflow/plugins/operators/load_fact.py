from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_query = "",
                 table = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table = table

    def execute(self, context):
        #self.log.info('LoadFactOperator not implemented yet')
        
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        self.log.info(f'Loading data into {self.table} fact table...')

        redshift_hook.run(self.sql_query)        
        self.log.info("Successful loading fact table!.")
