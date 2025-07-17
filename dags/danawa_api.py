from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 10),
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='danawa_etl_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['danawa', 'lambda', 'glue', 'snowflake']
) as dag:

    lambda_crawl = LambdaInvokeFunctionOperator(
        task_id='lambda_crawl',
        function_name='danawa_crawler_lambda',
        payload="""{
            "bucket": "de6-team8-bucket",
            "prefix": "raw_data/danawa/",
            "filename": "danawa_{{ ds }}.json"
        }""",
        aws_conn_id='aws_default',
        log_type='Tail',
        invocation_type='Event'
    )

    glue_transform = GlueJobOperator(
        task_id="glue_transform",
        job_name="de6-team8-danawa_json_to_parquet",
        script_location="s3://de6-team8-bucket/glue/scripts/danawa/danawa_json_to_parquet.py",
        iam_role_name="de6-team8-glue-role",
        region_name="ap-northeast-2",
        wait_for_completion=True
    )

    copy_to_snowflake = SnowflakeOperator(
        task_id='copy_to_snowflake',
        sql="""
            COPY INTO processed.danawa
            FROM @naver_stage/processed_data/danawa/parquet/date={{ ds }}/
            FILE_FORMAT = (TYPE = PARQUET)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            PATTERN = '.*\\.parquet$';
        """,
        snowflake_conn_id='team8_snowflake_conn',
    )

    lambda_crawl >> glue_transform >> copy_to_snowflake