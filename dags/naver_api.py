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
          SELECT
            t.$1:title::STRING       AS title,
            t.$1:link::STRING        AS link,
            t.$1:image::STRING       AS image,
            t.$1:lprice::STRING      AS lprice,
            t.$1:hprice::STRING      AS hprice,
            t.$1:mallName::STRING    AS mallName,
            t.$1:productId::STRING   AS productId,
            t.$1:productType::STRING AS productType,
            t.$1:brand::STRING       AS brand,
            t.$1:maker::STRING       AS maker,
            t.$1:category1::STRING   AS category1,
            t.$1:category2::STRING   AS category2,
            t.$1:category3::STRING   AS category3,
            t.$1:category4::STRING   AS category4,
            ROW_NUMBER() OVER (
              PARTITION BY t.$1:title::STRING
              ORDER BY metadata$filename
            ) AS rn
          FROM @naver_stage/date={{ ds }}/ (  
              FILE_FORMAT => (TYPE => 'PARQUET'),  
              PATTERN     => '.*\.parquet$'  
            ) t
        ) AS src
        ON target.title = src.title
        WHEN MATCHED AND src.rn = 1 THEN
          -- 중복인 경우 아무 업데이트도 하지 않음
          UPDATE SET title = target.title
        WHEN NOT MATCHED AND src.rn = 1 THEN
          INSERT (
            title, link, image, lprice, hprice,
            mallName, productId, productType,
            brand, maker, category1, category2,
            category3, category4
          )
          VALUES (
            src.title, src.link, src.image, src.lprice, src.hprice,
            src.mallName, src.productId, src.productType,
            src.brand, src.maker, src.category1, src.category2,
            src.category3, src.category4
          );
        """,
        snowflake_conn_id='team8_snowflake_conn',
    )

    lambda_crawl >> glue_transform >> copy_to_snowflake
