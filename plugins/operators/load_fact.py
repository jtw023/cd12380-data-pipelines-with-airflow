import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    songplays = """
        CREATE TABLE public.songplays (
            playid VARCHAR(32) NOT NULL,
            start_time TIMESTAMP NOT NULL,
            userid INT4 NOT NULL,
            "level" VARCHAR(256),
            songid VARCHAR(256),
            artistid VARCHAR(256),
            sessionid INT4,
            "location" VARCHAR(256),
            user_agent VARCHAR(256),
            CONSTRAINT songplays_pkey PRIMARY KEY (playid)
        );
    """

    insert_songplays = """
        INSERT INTO {}
        ({})
    """


    @apply_defaults
    def __init__(
        self, redshift_conn_id, table, sql, *args, **kwargs
    ):
        """
        Operator will create facts tables in Redshift

        What gets passed into the operator:
            :parameter redshift_conn_id: This is the connection ID of your Redshift connection within Airflow
            :parameter table: This is the table you would like Redshift to create
            :parameter sql: This is the sql query to execute
        """

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql

    def execute(self, context):
        logging.info("##### Connecting to Redshift #####")
        redshift = PostgresHook(self.redshift_conn_id)

        logging.info(f"##### Dropping {self.table} if it already exists #####")
        redshift.run(f"DROP TABLE IF EXISTS {self.table}")

        logging.info(f"##### Creating {self.table} #####")
        redshift.run(LoadFactOperator.songplays)

        logging.info(f"##### Inserting data into {self.table} #####")
        redshift.run(LoadFactOperator.insert_songplays.format(self.table, self.sql))
