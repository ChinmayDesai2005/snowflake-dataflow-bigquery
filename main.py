from snowflake.snowpark import Session
import apache_beam as beam
import apache_beam.io.snowflake as SnowflakeIO
import apache_beam.io.gcp.bigquery as BigQueryIO
from apache_beam.options.pipeline_options import PipelineOptions
from dotenv import load_dotenv
import pandas as pd
import os

load_dotenv()

ACCOUNT = os.environ.get('ACCOUNT')
USER = os.environ.get('USERNAME')
PASSWORD = os.environ.get('PASSWORD')
ROLE = os.environ.get('ROLE')


# Snowflake INFO 
SF_SERVER_NAME = 'https://yc83689.europe-west4.gcp.snowflakecomputing.com'
SF_DATABASE = 'TEST'
SF_SCHEMA = 'STEAM_DATA'
SF_TABLE_NAME = 'HOUSING_DATA'
SF_QUERY = f'SELECT * FROM {TABLE_NAME} LIMIT 30'
SF_INTEGRATION = 'MAIN_INTEGRATION'
SF_STAGING_BUCKET = 'gs://snowflake-dataflow-bigquery/'

# BigQuery INFO
BQ_TABLE = 'snowflake_data',
BQ_SCHEMA = 'SCHEMA_AUTODETECT',
BQ_DATASET = 'from_snowflake', 
BQ_PROJECT = 'proj-research-and-development', 
BQ_TEMP_LOCATION = 'gs://team-machine-learning-bucket/snowflake-bigquery/sub'


# GET Table Schema From Snowflake
connection_parameters = {
    "account": ACCOUNT,
    "user": USER,
    "password": PASSWORD,} 
session = Session.builder.configs(connection_parameters).create()
columns = session.sql(f'SHOW COLUMNS IN TABLE {DATABASE}.{SCHEMA}.{TABLE_NAME};')
columns = pd.DataFrame(columns.collect())['column_name'].to_list()

def csv_mapper(strings_array):
    return {columns[i]: element.decode("utf-8") for i, element in enumerate(strings_array)}

options = PipelineOptions()

# Main Pipeline
with beam.Pipeline(options=options) as pipeline:
    items = ( pipeline | SnowflakeIO.ReadFromSnowflake(server_name=SF_SERVER_NAME,
                                                        database=SF_DATABASE,
                                                        schema=SF_SCHEMA,
                                                        role=ROLE,
                                                        query=SF_QUERY,
                                                        storage_integration_name=SF_INTEGRATION,
                                                        staging_bucket_name=SF_STAGING_BUCKET,
                                                        username=USER,
                                                        password=PASSWORD,
                                                        csv_mapper=csv_mapper))
    items | BigQueryIO.WriteToBigQuery(table=BQ_TABLE,
                                        schema=BQ_SCHEMA,
                                        dataset=BQ_DATASET, 
                                        project=BQ_PROJECT, 
                                        custom_gcs_temp_location=BQ_TEMP_LOCATION, 
                                        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)