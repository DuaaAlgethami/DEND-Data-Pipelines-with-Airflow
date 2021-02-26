from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 redshift_conn_id="",
                 aws_credentials_id="",
                 region="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json = json
        self.aws_credentials_id = aws_credentials_id
        self.region = region

    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        #STEP2: clear existing raw data
        self.log.info("Deleting data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
        
        #STEP3: copy the new raw data
        self.log.info("Copying data from S3 to Redshift")
        
        copy_to_redshift = ("""
                                copy {table}
                                from 's3://{table}/{s3_bucket}'
                                access_key_id '{access_key}' secret_access_key '{secret_key}'
                                compupdate off region '{region}'
                                FORMAT AS JSON '{json}'
                                TIMEFORMAT as 'epochmillisecs';
                                """.format(
                                table =self.table, s3_bucket=self.s3_bucket, s3_key=self.s3_key, 
                                access_key=credentials.access_key, secret_key=credentials.secret_key,
                                region=self.region,
                                json=self.json)
                            )


        redshift.run(copy_to_redshift)

        self.log.info("Successful runing Copy command ")





