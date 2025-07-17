from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
import time
import os
import re
import json

# ─────────────────────────────
# DAG 기본 설정
# ─────────────────────────────
default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

YEAR = datetime.today().year
MONTH = datetime.today().month

# ─────────────────────────────
# 공통 유틸
# ─────────────────────────────
def get_soup_from_page_with(url):
    headers = {'User-Agent': 'Mozilla/5.0'}
    res = requests.get(url, headers=headers)
    return bs(res.text, 'lxml')

def is_empty_page(soup):
    return bool(soup.select_one("i.fa.fa-exclamation-triangle"))

def isBlocked(soup):
    return bool(soup.select_one("h2.title"))

def parse_views(views):
    if views.isdigit():
        return int(views)
    unit_map = {'k': 1_000, 'm': 1_000_000}
    return int(float(views[:-1]) * unit_map.get(views[-1].lower(), 1))

def parse_date(created_at):
    global YEAR, MONTH
    today = datetime.today().strftime('%Y-%m-%d')
    if re.search(r'(방금|분 전|\d+분|\d+시간)', created_at) or re.match(r'\d{2}:\d{2}', created_at):
        return today
    elif re.match(r'\d{2}-\d{2}', created_at):
        month = int(created_at[:2])
        if MONTH == 1 and month == 12:
            YEAR -= 1
        if MONTH != month:
            MONTH = month
        return f"{YEAR}-{created_at}"
    else:
        raise ValueError(f"알 수 없는 날짜 형식: {created_at}")

def get_hotdeal_summary(hotdeal):
    votes = hotdeal.select_one("td > span.num.num")
    title = hotdeal.select_one("a.subject-link span.ellipsis-with-reply-cnt")
    price = hotdeal.select_one("span.text-orange")
    views = hotdeal.select_one("span.count")
    created = hotdeal.select_one("span.date")

    if not all([votes, title, price, views, created]):
        return None

    return {
        "votes": votes.text.strip(),
        "title": title.text.strip(),
        "price": price.text.strip(),
        "views": parse_views(views.text.strip()),
        "created_at": parse_date(created.text.strip())
    }

def scrap_hotdeal_info(soup, hotdeal_info, category_name):
    rows = soup.select("table > tbody > tr")
    for hotdeal in rows:
        summary = get_hotdeal_summary(hotdeal)
        if summary:
            summary["category"] = category_name
            hotdeal_info.append(summary)

# ─────────────────────────────
# 크롤링 Task
# ─────────────────────────────
def crawl_quasarzone_category(category, base_url):
    today_str = datetime.today().strftime("%Y-%m-%d")
    until_date = datetime(2024, 1, 1)

    page = 1
    hotdeal_info = []
    global YEAR, MONTH
    YEAR = datetime.today().year
    MONTH = datetime.today().month

    while True:
        url = base_url.format(page)
        soup = get_soup_from_page_with(url)

        if is_empty_page(soup):
            break
        if isBlocked(soup):
            time.sleep(10)
            continue

        scrap_hotdeal_info(soup, hotdeal_info, category)

        last_scraped = datetime.strptime(hotdeal_info[-1]['created_at'], "%Y-%m-%d")
        if last_scraped < until_date:
            break

        print(f"🌀 현재 페이지: {page}, 누적 수집 수: {len(hotdeal_info)}")
        page += 1
        time.sleep(1)

    df = pd.DataFrame(hotdeal_info)
    df = df[df['created_at'] >= until_date.strftime('%Y-%m-%d')]
    df = df.sort_values(by="created_at")

    if not df.empty:
        file_name = f"{today_str}.json"
        local_path = f"/tmp/{file_name}"

        df.to_json(local_path, orient="records", force_ascii=False, indent=2)

        s3 = S3Hook(aws_conn_id='aws_default')
        s3_key = f"raw_data/quasarzone/{category}/{file_name}"
        s3.load_file(local_path, bucket_name="de6-team8-bucket", key=s3_key, replace=True)

        print(f"✅ S3 업로드 완료: {s3_key}")
        print(f"🔁 총 수집 페이지: {page}")
    else:
        print("📭 신규 데이터 없음")

# ─────────────────────────────
# DAG 정의
# ─────────────────────────────
with DAG(
    dag_id='quasarzone_etl',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2025, 7, 10),
    catchup=False,
    tags=['quasarzone', 'etl']
) as dag:

    task_pc_hardware = PythonOperator(
        task_id='crawl_pc_hardware',
        python_callable=crawl_quasarzone_category,
        op_args=[
            "pc_hardware",
            "https://quasarzone.com/bbs/qb_saleinfo?_method=post&_token=xxx&category=PC%2F%ED%95%98%EB%93%9C%EC%9B%A8%EC%96%B4&kind=subject&sort=num%2C+reply&direction=DESC&page={}"
        ]
    )

    task_notebook_mobile = PythonOperator(
        task_id='crawl_notebook_mobile',
        python_callable=crawl_quasarzone_category,
        op_args=[
            "notebook_mobile",
            "https://quasarzone.com/bbs/qb_saleinfo?_method=post&_token=xxx&category=%EB%85%B8%ED%8A%B8%EB%B6%81%2F%EB%AA%A8%EB%B0%94%EC%9D%BC&kind=subject&sort=num%2C+reply&direction=DESC&page={}"
        ]
    )

    glue_transform = GlueJobOperator(
        task_id="run_glue_job",
        job_name="de6-team8-testjob",
        script_location="s3://de6-team8-bucket/glue/scripts/quasarzone/quasarzone_json_to_parquet.py",
        iam_role_name="de6-team8-glue-role",
        region_name="ap-northeast-2",
        wait_for_completion=True
    )


    
    
    merge_into_snowflake = SnowflakeOperator(
    task_id='merge_into_snowflake',
    sql=f"""
    MERGE INTO processed.quasarzone AS target
    USING (
      SELECT * FROM @quasarzone_stage/de6-team8-testjob-{datetime.today().strftime("%Y-%m-%d")}/ 
      (FILE_FORMAT => (TYPE => 'PARQUET'))
    ) AS source
    ON target.title = source.title AND target.created_at = source.created_at
    WHEN NOT MATCHED THEN
      INSERT (votes, title, price, views, created_at, category)
      VALUES (source.votes, source.title, source.price, source.views, source.created_at, source.category);
    """,
    snowflake_conn_id='team8_snowflake_conn',
)

    
    


    [task_pc_hardware, task_notebook_mobile] >> glue_transform >> merge_into_snowflake
