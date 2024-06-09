import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    user = """
        CREATE TABLE public.artists (
            artistid VARCHAR(256) NOT NULL,
            "name" VARCHAR(512),
            "location" VARCHAR(512),
            lattitude NUMERIC(18, 0),
            longitude NUMERIC(18, 0)
        );
    """

    song = """
        CREATE TABLE public.songs (
            songid VARCHAR(256) NOT NULL,
            title VARCHAR(512),
            artistid VARCHAR(256),
            "year" INT4,
            duration NUMERIC(18, 0),
            CONSTRAINT songs_pkey PRIMARY KEY (songid)
        );
    """

    artist = """
        CREATE TABLE public.artists (
            artistid VARCHAR(256) NOT NULL,
            "name" VARCHAR(512),
            "location" VARCHAR(512),
            lattitude NUMERIC(18, 0),
            longitude NUMERIC(18, 0)
        );
    """

    time = """
        CREATE TABLE public."time" (
            start_time TIMESTAMP NOT NULL,
            "hour" INT4,
            "day" INT4,
            "week" INT4,
            "month" VARCHAR(256),
            "year" INT4,
            "weekday" VARCHAR(256),
            CONSTRAINT time_pkey PRIMARY KEY (start_time)
        ) ;
    """

    insert = """
        INSERT INTO {}
        ({})
    """

    @apply_defaults
    def __init__(
        self, redshift_conn_id, table, sql, *args, **kwargs
    ):
        """
        Operator will create dimension tables in Redshift

        What gets passed into the operator:
            :parameter redshift_conn_id: This is the connection ID of your Redshift connection within Airflow
            :parameter table: This is the table you would like Redshift to create
            :parameter sql: This is the sql query to execute
        """

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql

    def execute(self, context):
        logging.info("##### Connecting to Redshift #####")
        redshift = PostgresHook(self.redshift_conn_id)

        logging.info(f"##### Truncating {self.table} if it already exists #####")
        redshift.run(f"TRUNCATE TABLE IF EXISTS {self.table}")

        logging.info(f"##### Creating {self.table} #####")
        if self.table == 'users':
            redshift.run(LoadDimensionOperator.user)
        elif self.table == 'songs':
            redshift.run(LoadDimensionOperator.song)
        elif self.table == 'artists':
            redshift.run(LoadDimensionOperator.artist)
        elif self.table == 'time':
            redshift.run(LoadDimensionOperator.time)

        logging.info(f"##### Inserting data into {self.table} #####")
        redshift.run(LoadDimensionOperator.insert.format(self.table, self.sql))
