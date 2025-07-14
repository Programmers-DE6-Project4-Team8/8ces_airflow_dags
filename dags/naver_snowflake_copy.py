from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2025, 7, 1),
}

with DAG(
    dag_id='copy_parquet_to_snowflake',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['team8', 'snowflake'],
) as dag:

    copy_into_snowflake = SnowflakeOperator(
        task_id='copy_from_s3_parquet',
        sql="""
            COPY INTO processed.naver
            FROM @naver_stage
            FILE_FORMAT = (TYPE = PARQUET)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            PATTERN = '.*\\.parquet$';
        """,
        snowflake_conn_id='team8_snowflake_conn',
    )
