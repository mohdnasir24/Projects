from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    
    select_sql = "SELECT count(1) as record_count from {} where {} is null;"

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.tables_cols = kwargs["tables_cols"]

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for key_tables_cols in self.tables_cols:
            
            formatted_sql = DataQualityOperator.select_sql.format(
            key_tables_cols,
            self.tables_cols[key_tables_cols]
            )
            
            records=redshift.get_records(formatted_sql)
            len_records=len(records)
            num_records = records[0][0]
            
            if len_records == 1 and num_records == 0:
                self.log.info(f"Data quality on table {key_tables_cols} check passed with {records[0][0]} null records on {self.tables_cols[key_tables_cols]} \
                column ")
            else :
                raise ValueError(f"Data quality check failed. {table} contains {records[0][0]} null records on {self.tables_cols[key_tables_cols]} \
                column ")
            
           
            
          
            