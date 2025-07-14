from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import os
import json
import requests
from bs4 import BeautifulSoup as bs
import time
import pandas as pd
import re

# DAG 기본 설정
default_args = {
    'start_date': datetime(2025, 7, 10),
    'retries': 1,
}

# 날짜 포맷
today_str = datetime.now().strftime("%Y-%m-%d")
bucket = "de6-team8-bucket"

# 🔽 공통 크롤러 함수 정의
def crawl_category(category_name, base_url):
    state_path = f"/opt/airflow/dags/state/{category_name}_state.json"
    state = {}
    if os.path.exists(state_path):
        with open(state_path, 'r') as f:
            state = json.load(f)

    year = datetime.today().year
    month = datetime.today().month
    last_collected = datetime.strptime(state.get(category_name, "2022-01-01"), "%Y-%m-%d")
    hotdeal_info = []
    page = 1

    while True:
        url = base_url.format(page)
        resp = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
        soup = bs(resp.text, "lxml")

        rows = soup.select("div.list-board-wrap tbody tr")
        if not rows or "fa-exclamation-triangle" in soup.text:
            break

        for row in rows:
            try:
                votes = row.select_one("span.num.num").text.strip()
                title = row.select_one("a.subject-link span").text.strip()
                price = row.select_one("span.text-orange").text.strip()
                views = row.select_one("span.count").text.strip()
                created_at = row.select_one("span.date").text.strip()

                # 날짜 파싱
                if re.match(r'\d{2}:\d{2}', created_at) or '시간 전' in created_at:
                    created_at = datetime.today().strftime("%Y-%m-%d")
                elif re.match(r'\d{2}-\d{2}', created_at):
                    created_at = f"{year}-{created_at}"

                if datetime.strptime(created_at, "%Y-%m-%d") <= last_collected:
                    break

                hotdeal_info.append({
                    "votes": votes,
                    "title": title,
                    "price": price,
                    "views": int(re.sub(r"[^\d]", "", views)),
                    "created_at": created_at,
                    "category": category_name
                })

            except Exception:
                continue

        page += 1
        time.sleep(2)

    if hotdeal_info:
        df = pd.DataFrame(hotdeal_info).drop_duplicates()
        filename = f"{category_name}/{today_str}.json"
        local_path = f"/tmp/{category_name}_{today_str}.json"
        df.to_json(local_path, orient='records', force_ascii=False, indent=2)

        # S3 업로드
        s3_hook = S3Hook(aws_conn_id='aws_default')
        s3_hook.load_file(
            filename=local_path,
            key=f"raw_data/quasarzone/{filename}",
            bucket_name=bucket,
            replace=True
        )

        # state 업데이트
        state[category_name] = df["created_at"].max()
        os.makedirs("/opt/airflow/dags/state", exist_ok=True)
        with open(state_path, 'w') as f:
            json.dump(state, f, indent=2)

# DAG 정의
with DAG(
    dag_id="quasarzone_full_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["quasarzone", "glue", "snowflake"]
) as dag:

    crawl_pc = PythonOperator(
        task_id="crawl_quasarzone_pc",
        python_callable=crawl_category,
        op_args=[
            "pc_hardware",
            "https://quasarzone.com/bbs/qb_saleinfo?_method=post&_token=xxx&category=PC%2F%ED%95%98%EB%93%9C%EC%9B%A8%EC%96%B4&kind=subject&sort=num%2C+reply&direction=DESC&page={}"
        ],
    )

    crawl_notebook = PythonOperator(
        task_id="crawl_quasarzone_notebook",
        python_callable=crawl_category,
        op_args=[
            "notebook_mobile",
            "https://quasarzone.com/bbs/qb_saleinfo?_method=post&_token=xxx&category=%EB%85%B8%ED%8A%B8%EB%B6%81%2F%EB%AA%A8%EB%B0%94%EC%9D%BC&kind=subject&sort=num%2C+reply&direction=DESC&page={}"
        ],
    )

    glue_task = GlueJobOperator(
        task_id="glue_transform_task",
        job_name="de6-team8-testjob",
        script_location="s3://de6-team8-bucket/glue/scripts/quasarzone/quasarzone_json_to_parquet.py",
        iam_role_name="de6-team8-glue-role",
        region_name="ap-northeast-2",
        wait_for_completion=True,
    )

    copy_to_snowflake = SnowflakeOperator(
        task_id='copy_to_snowflake',
        sql=f"""
        COPY INTO processed.quasarzone
        FROM @quasarzone_stage/de6-team8-testjob-{today_str}/
        FILE_FORMAT = (TYPE = PARQUET)
        PATTERN = '.*\\.parquet$';
        """,
        snowflake_conn_id='team8_snowflake_conn',
    )

    [crawl_pc, crawl_notebook] >> glue_task >> copy_to_snowflake
