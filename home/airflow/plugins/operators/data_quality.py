from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="redshift",
                 dq_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        
        self.redshift_conn_id=redshift_conn_id
        #self.tables = tables
        self.checks=dq_checks

    def execute(self, context):
        #self.log.info('DataQualityOperator not implemented yet')
        
        redshift_hook = PostgresHook('redshift')
        
       '''' for table in self.tables:
            
            self.log.info(f"Starting data quality validation on table : {table}")
            records = redshift_hook.get_records(f"select count(*) from {table};")

            if len(records) < 1 or len(records[0]) < 1 or records[0][0] < 1:
                self.log.error(f"Data Quality validation failed for table : {table}.")
                raise ValueError(f"Data Quality validation failed for table : {table}")
            self.log.info(f"Data Quality Validation succeds on table : {table}")
            '''
            
            for check in self.dq_checks:
                sql = check.get('check_sql')
                exp_result = check.get('expected_result')
                
                records = redshift.get_records(sql)[0]
                
                if exp_result != records[0]:
                    error_count += 1
                    failing_tests.append(sql)
                    
                if error_count > 0:
                    self.log.info('Tests failed')
                    self.log.info(failing_tests)
                    raise ValueError('Data quality check failed')
                else:
                    self.log.info(f"Data quality on SQL {sql} check passed with {records[0][0]} records")