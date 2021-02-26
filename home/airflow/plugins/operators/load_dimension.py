from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 table,
                 redshift_conn_id='redshift',
                 sql_query ='',
                 mode='append',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query  = sql_query 
        self.mode = mode

    def execute(self, context):
        #self.log.info('LoadDimensionOperator not implemented yet')
        
        redshift_hook = PostgresHook('redshift')
        
        if self.mode == 'truncate':
            self.log.info(f'Deleting data from {self.table} dimension table...')
            redshift_hook.run(f'DELETE FROM {self.table};')
            self.log.info('Successful Deleting dimension table!')

       
        self.log.info(f'Loading data into {self.table} dimension table...')
        redshift_hook.run(self.sql_query)
        self.log.info('Successful loading dimension table!')
        
        
