import pendulum
import os
import logging

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from operators import (
        StageToRedshiftOperator, LoadFactOperator,
        LoadDimensionOperator, DataQualityOperator
    )
from helpers import SqlQueries

default_args = {
    'Owner': 'Jordan Walters',
    'Depends_on_past': False,
    'Start_date': pendulum.now(),
    'Retries': 3,
    'Retry_delay': timedelta(minutes=5),
    'Catchup': False,
    'Email_on_retry': False,
    'Email_on_failure': False,
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly'
)
def final_project():

    @task
    def begin_execution():
        DummyOperator(task_id='Begin_execution')
        logging.info("##### Let's begin #####")

    @task
    def end_execution():
        DummyOperator(task_id='End_execution')
        logging.info("##### All done #####")

    @task
    def stage_logs_to_redshift():
        StageToRedshiftOperator(
            task_id='Stage_logs',
            redshift_conn_id='redshift',
            aws_conn_id='airflow_user',
            s3_bucket='jw-airflow-test',
            s3_key='log_data/{execution_date.year}/{execution_date.month}/{ds}',
            table='staging_logs',
            jsonpath='log_json_path.json',
        )


    @task
    def stage_songs_to_redshift():
        StageToRedshiftOperator(
            task_id='Stage_songs',
            redshift_conn_id='redshift',
            aws_conn_id='airflow_user',
            s3_bucket='jw-airflow-test',
            s3_key='song_data',
            table='staging_songs',
            jsonpath='log_json_path.json',
        )

    @task
    def load_songplays_table():
        LoadFactOperator(
            task_id='load_songplays_fact_table',
            redshift_conn_id='redshift',
            table='songplays',
            sql=SqlQueries.songplay_table_insert,
        )

    @task
    def load_user_dimension_table():
        LoadDimensionOperator(
            task_id='load_user_dim_table',
            redshift_conn_id='redshift',
            table='users',
            sql=SqlQueries.user_table_insert,
        )

    @task
    def load_song_dimension_table():
        LoadDimensionOperator(
            task_id='load_song_dim_table',
            redshift_conn_id='redshift',
            table='songs',
            sql=SqlQueries.song_table_insert,
        )

    @task
    def load_artist_dimension_table():
        LoadDimensionOperator(
            task_id='load_artist_dim_table',
            redshift_conn_id='redshift',
            table='artists',
            sql=SqlQueries.artist_table_insert,
        )

    @task
    def load_time_dimension_table():
        LoadDimensionOperator(
            task_id='load_time_dim_table',
            redshift_conn_id='redshift',
            table='time',
            sql=SqlQueries.time_table_insert,
        )

    @task
    def run_quality_checks():
        DataQualityOperator(
            task_id='Run_data_quality_checks',
            redshift_conn_id="redshift",
            tables=[
                "songplays", 
                "songs",
                "users",
                "artists",
                "time",
            ],
            sql=SqlQueries.check_quality,
        )

    begin_execution_task=begin_execution()
    stage_logs_to_redshift_task=stage_logs_to_redshift()
    stage_songs_to_redshift_task=stage_songs_to_redshift()
    load_songplays_table_task=load_songplays_table()
    load_user_dimension_table_task=load_user_dimension_table()
    load_song_dimension_table_task=load_song_dimension_table()
    load_artist_dimension_table_task=load_artist_dimension_table()
    load_time_dimension_table_task=load_time_dimension_table()
    run_quality_checks_task=run_quality_checks()
    end_execution_task=end_execution()

    begin_execution_task >> stage_logs_to_redshift_task >> load_songplays_table_task
    begin_execution_task >> stage_songs_to_redshift_task >> load_songplays_table_task
    load_songplays_table_task >> load_user_dimension_table_task
    load_songplays_table_task >> load_song_dimension_table_task
    load_songplays_table_task >> load_artist_dimension_table_task
    load_songplays_table_task >> load_time_dimension_table_task
    load_user_dimension_table_task >> run_quality_checks_task >> end_execution_task
    load_song_dimension_table_task >> run_quality_checks_task >> end_execution_task
    load_artist_dimension_table_task >> run_quality_checks_task >> end_execution_task
    load_time_dimension_table_task >> run_quality_checks_task >> end_execution_task

final_project_dag=final_project()
