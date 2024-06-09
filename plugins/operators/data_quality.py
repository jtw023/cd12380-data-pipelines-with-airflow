import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(
        self, redshift_conn_id, tables, sql, *args, **kwargs
    ):
        """
        Operator will check data quality for tables given - currently runs SELECT COUNT(*)

        What gets passed into the operator:
            :parameter redshift_conn_id: This is the connection ID of your Redshift connection within Airflow
            :parameter tables: This is a list of tables for the operator to check
            :parameter sql: This is the sql query to execute
        """

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.sql = sql

    def execute(self, context):
        logging.info("##### Connecting to Redshift #####")
        redshift = PostgresHook(self.redshift_conn_id)

        for table in self.tables:
            logging.info(f"##### Checking data quality for {table} #####")
            result = redshift.run(self.sql.format(table))
            if result < 1:
                logging.info(f"{table} did not pass!!")
                raise ValueError(f"{table} did not pass!!")

        logging.info("##### All tables passed successfully #####")
