from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 10),
    'retries': 1,
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

    # lambda_crawl = LambdaInvokeFunctionOperator(
    #     task_id='lambda_crawl',
    #     function_name='naver_shopping_crawler_lambda',
    #     payload="""{
    #         "categories": [
    #             "CPU", "GPU", "RAM", "SSD", "HDD",
    #             "메인보드", "파워서플라이",
    #             "사무용 노트북", "게이밍 노트북",
    #             "스마트폰", "태블릿", "이어폰"
    #         ],
    #         "bucket": "de6-team8-bucket",
    #         "prefix": "raw_data/naver/",
    #         "filename": "naver_{{ ds }}.json"
    #     }""",
    #     aws_conn_id='aws_default',
    #     log_type='Tail',
    #     invocation_type='Event'
    # )

    # glue_transform = GlueJobOperator(
    #     task_id="glue_transform",
    #     job_name="de6-team8-naver_json_to_parquet",
    #     script_location="s3://de6-team8-bucket/glue/scripts/naver/naver_json_to_parquet.py",
    #     iam_role_name="de6-team8-glue-role",
    #     region_name="ap-northeast-2",
    #     wait_for_completion=True
    # )

    copy_to_snowflake = SnowflakeOperator(
        task_id='copy_to_snowflake',
        sql="""
            CREATE OR REPLACE EXTERNAL TABLE ext_naver_shopping (
                  title        STRING,
                  link         STRING,
                  image        STRING,
                  lprice       STRING,
                  hprice       STRING,
                  mallName     STRING,
                  productId    STRING,
                  productType  STRING,
                  brand        STRING,
                  maker        STRING,
                  category1    STRING,
                  category2    STRING,
                  category3    STRING,
                  category4    STRING
                )
                WITH LOCATION=@naver_stage/date={{ ds }}/
                FILE_FORMAT=(TYPE='PARQUET');
            MERGE INTO processed.naver AS target
                USING (
                  SELECT
                    t.$1,  t.$2,  t.$3,  t.$4,
                    t.$5,  t.$6,  t.$7,  t.$8,
                    t.$9,  t.$10, t.$11, t.$12,
                    t.$13, t.$14
                  FROM @naver_stage/date={{ ds }}/
                    ( 
                        FILE_FORMAT => naver_parquet_fmt,
                        MATCH_BY_COLUMN_NAME   => 'CASE_INSENSITIVE'
                    )
                  AS t
                ) AS source (
                  title,
                  link,
                  image,
                  lprice,
                  hprice,
                  mallName,
                  productId,
                  productType,
                  brand,
                  maker,
                  category1,
                  category2,
                  category3,
                  category4
                )
                  ON target.title = source.title
                WHEN NOT MATCHED THEN
                  INSERT (
                    title, link, image, lprice, hprice, mallName,
                    productId, productType, brand, maker,
                    category1, category2, category3, category4
                  )
                  VALUES (
                    source.title, source.link, source.image, source.lprice, source.hprice, source.mallName,
                    source.productId, source.productType, source.brand, source.maker,
                    source.category1, source.category2, source.category3, source.category4
                  )
                ;

        """,
        snowflake_conn_id='team8_snowflake_conn',
    )

    # lambda_crawl >> glue_transform >> copy_to_snowflake
    copy_to_snowflake
