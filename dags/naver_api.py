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

    # create_ext_table = SnowflakeOperator(
    #     task_id='create_ext_table',
    #     snowflake_conn_id='team8_snowflake_conn',
    #     sql="""
    #     CREATE OR REPLACE EXTERNAL TABLE ext_naver_shopping (
    #       title        STRING AS ( VALUE:"title"       ::STRING ),
    #       link         STRING AS ( VALUE:"link"        ::STRING ),
    #       image        STRING AS ( VALUE:"image"       ::STRING ),
    #       lprice       STRING AS ( VALUE:"lprice"      ::STRING ),
    #       hprice       STRING AS ( VALUE:"hprice"      ::STRING ),
    #       mallName     STRING AS ( VALUE:"mallName"    ::STRING ),
    #       productId    STRING AS ( VALUE:"productId"   ::STRING ),
    #       productType  STRING AS ( VALUE:"productType" ::STRING ),
    #       brand        STRING AS ( VALUE:"brand"       ::STRING ),
    #       maker        STRING AS ( VALUE:"maker"       ::STRING ),
    #       category1    STRING AS ( VALUE:"category1"   ::STRING ),
    #       category2    STRING AS ( VALUE:"category2"   ::STRING ),
    #       category3    STRING AS ( VALUE:"category3"   ::STRING ),
    #       category4    STRING AS ( VALUE:"category4"   ::STRING )
    #     )
    #     WITH LOCATION = @naver_stage/date={{ ds }}/
    #     FILE_FORMAT = (TYPE = 'PARQUET')
    #     AUTO_REFRESH = FALSE;
    #     """
    # )

    # 2) 외부 테이블을 대상으로 MERGE 수행
    merge_to_snowflake = SnowflakeOperator(
        task_id='merge_to_snowflake',
        snowflake_conn_id='team8_snowflake_conn',
        sql="""
        MERGE INTO processed.naver AS target
        USING ext_naver_shopping AS source
          ON target.title = source.title
        WHEN NOT MATCHED THEN
          INSERT (
            title, link, image, lprice, hprice, mallName,
            productId, productType, brand, maker,
            category1, category2, category3, category4
          )
          VALUES (
            source.title, source.link, source.image,
            source.lprice, source.hprice, source.mallName,
            source.productId, source.productType, source.brand,
            source.maker, source.category1, source.category2,
            source.category3, source.category4
          );
        """
    )

    # 순서 지정
    
    lambda_crawl >> glue_transform >> merge_to_snowflake
