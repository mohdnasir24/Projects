from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator, PostgresOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup' : False,
    'email_on_failure': False,
    'start_date': datetime(2021, 7, 12),
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@daily',
          template_searchpath =['/home/workspace/airflow']
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql='create_tables.sql',
    autocommit=True
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",    
    sql = SqlQueries.songplay_table_insert,
    table = "songplays"
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql = SqlQueries.user_table_insert,
    table = "users"
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql = SqlQueries.song_table_insert,
    table = "songs"
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql = SqlQueries.artist_table_insert,
    table = "artists"
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql = SqlQueries.time_table_insert,
    table = "time"
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials_id="aws_credentials",
    tables_cols = {'artists': 'artistid', 
              'songs': 'songid', 
              'time': 'start_time', 
              'songplays': 'playid', 
              'users': 'userid'}    
    
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator>>create_tables
create_tables>>stage_events_to_redshift
create_tables>>stage_songs_to_redshift
stage_events_to_redshift>>load_songplays_table
stage_songs_to_redshift>>load_songplays_table
load_songplays_table>>load_song_dimension_table
load_songplays_table>>load_user_dimension_table
load_songplays_table>>load_artist_dimension_table
load_songplays_table>>load_time_dimension_table
load_song_dimension_table>>run_quality_checks
load_user_dimension_table>>run_quality_checks
load_artist_dimension_table>>run_quality_checks
load_time_dimension_table>>run_quality_checks
run_quality_checks>>end_operator


