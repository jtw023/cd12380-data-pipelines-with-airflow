import logging
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)

    staging_sql = """
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            IGNORE HEADER 1
            REGION 'us-east-1'
            JSON '{}'
            """

    staging_logs = """
        CREATE TABLE public.staging_logs (
            artist VARCHAR(256),
            auth VARCHAR(256),
            firstname VARCHAR(256),
            gender VARCHAR(256),
            iteminsession INT4,
            lastname VARCHAR(256),
            length NUMERIC(18, 0),
            "level" VARCHAR(256),
            location VARCHAR(256),
            "method" VARCHAR(256),
            page VARCHAR(256),
            registration NUMERIC(18, 0),
            sessionid INT4,
            song VARCHAR(256),
            status INT4,
            ts INT8,
            useragent VARCHAR(256),
            userid INT4
        );
    """

    staging_songs = """
        CREATE TABLE public.staging_songs (
            num_songs INT4,
            artist_id VARCHAR(256),
            artist_name VARCHAR(512),
            artist_latitude NUMERIC(18, 0),
            artist_longitude NUMERIC(18, 0),
            artist_location VARCHAR(512),
            song_id VARCHAR(256),
            title VARCHAR(512),
            duration NUMERIC(18, 0),
            "year" INT4
        );
    """


    @apply_defaults
    def __init__(
        self, redshift_conn_id, aws_conn_id,
        s3_bucket, s3_key, table, jsonpath, *args, **kwargs
    ):
        """
        Operator Will Load Data from S3 to Redshift

        What gets passed into the operator:
            :parameter redshift_conn_id: This is the connection ID of your Redshift connection within Airflow
            :parameter aws_conn_id: This is the connection ID of your AWS connection within Airflow
            :s3_bucket: This is the s3 bucket that already has data
            :s3_key: This is the path to your data from the s3 bucket
            :table: This is the table that you would like redshift to stage your s3 data to
            :jsonpath: This is your jsonpath file
        """

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.jsonpath = jsonpath

    def execute(self, context):
        logging.info("##### Connecting to AWS #####")
        aws = AwsHook(self.aws_conn_id)
        aws_creds = aws.get_credentials()

        logging.info("##### Connecting to Redshift #####")
        redshift = PostgresHook(self.redshift_conn_id)

        logging.info(f"##### Truncating {self.table} if it already exists #####")
        redshift.run(f"TRUNCATE TABLE IF EXISTS {self.table}")

        logging.info(f"##### Creating {self.table} #####")
        if self.table == 'staging_logs':
            redshift.run(StageToRedshiftOperator.staging_logs)
        elif self.table == 'staging_songs':
            redshift.run(StageToRedshiftOperator.staging_songs)


        logging.info("##### Copying data from S3 to Redshift #####")
        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"
        sql = StageToRedshiftOperator.staging_sql.format(
                self.table,
                s3_path,
                aws_creds.access_key,
                aws_creds.secret_key,
                f"s3://{self.s3_bucket}/{self.jsonpath}",
        )
        redshift.run(sql)





