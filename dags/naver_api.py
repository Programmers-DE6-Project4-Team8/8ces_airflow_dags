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
    dag_id='naver_shopping_etl_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['naver', 'lambda', 'glue', 'snowflake'],
    max_active_runs=1,
) as dag:

    lambda_crawl = LambdaInvokeFunctionOperator(
        task_id='lambda_crawl',
        function_name='naver_shopping_crawler_lambda',
        payload="""{
            "categories": [
                "CPU", "GPU", "RAM", "SSD", "HDD",
                "메인보드", "파워서플라이",
                "사무용 노트북", "게이밍 노트북",
                "스마트폰", "태블릿", "이어폰"
            ],
            "bucket": "de6-team8-bucket",
            "prefix": "raw_data/naver/",
            "filename": "naver_{{ ds }}.json"
        }""",
        aws_conn_id='aws_default',
        log_type='Tail',
        invocation_type='Event'
    )

    glue_transform = GlueJobOperator(
        task_id="glue_transform",
        job_name="de6-team8-naver_json_to_parquet",
        script_location="s3://de6-team8-bucket/glue/scripts/naver/naver_json_to_parquet.py",
        iam_role_name="de6-team8-glue-role",
        region_name="ap-northeast-2",
        wait_for_completion=True
    )

    copy_to_snowflake = SnowflakeOperator(
        task_id='copy_to_snowflake',
        sql="""
        MERGE INTO processed.naver AS target
        USING (
          -- 스테이지에서 오늘 파일만 읽어와 JSON 필드를 컬럼으로 파싱
          SELECT
            t.$1:col1::STRING      AS col1,
            t.$1:col2::NUMBER      AS col2,
            t.$1:title::STRING     AS title,
            '{{ ds }}'             AS ingestion_date,
            metadata$filename      AS src_file,
            ROW_NUMBER() OVER (
              PARTITION BY t.$1:title::STRING
              ORDER BY metadata$filename
            ) AS rn
          FROM @naver_stage/date='{{ ds }}'/ (FILE_FORMAT => 'PARQUET') t
        )
        AS src
        ON target.title = src.title
        -- 동일 title 이면서 가장 첫 번째(rn = 1) 레코드만 신규로 INSERT
        WHEN MATCHED AND src.rn = 1 THEN
          -- 아무 작업도 하지 않음 (중복 덮어쓰기 방지)
          UPDATE SET title = target.title
        WHEN NOT MATCHED AND src.rn = 1 THEN
          INSERT (col1, col2, title, ingestion_date)
          VALUES (src.col1, src.col2, src.title, src.ingestion_date);
        """,
        snowflake_conn_id='team8_snowflake_conn',
    )

    lambda_crawl >> glue_transform >> copy_to_snowflake
