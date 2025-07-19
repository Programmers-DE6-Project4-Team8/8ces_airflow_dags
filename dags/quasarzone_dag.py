from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
import time
import re
import boto3

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

YEAR = datetime.today().year
MONTH = datetime.today().month

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

# ✅ parquet 경로 삭제 함수
def delete_existing_parquet_files(**kwargs):
    date_str = datetime.today().strftime('%Y-%m-%d')
    prefixes = [
        f'processed_data/quasarzone/parquet/de6-team8-json-to-parquet-{date_str}/',
        f'processed_data/quasarzone/parquet/de6-team8-cleaning-job-{date_str}/'
    ]
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('de6-team8-bucket')
    for prefix in prefixes:
        bucket.objects.filter(Prefix=prefix).delete()
        print(f"🧹 삭제 완료: {prefix}")

# DAG 정의
with DAG(
    dag_id='quasarzone_full_etl',
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
            "https://quasarzone.com/bbs/qb_saleinfo?...page={}"
        ]
    )

    task_notebook_mobile = PythonOperator(
        task_id='crawl_notebook_mobile',
        python_callable=crawl_quasarzone_category,
        op_args=[
            "notebook_mobile",
            "https://quasarzone.com/bbs/qb_saleinfo?...page={}"
        ]
    )

    clear_s3_parquet = PythonOperator(
        task_id='clear_s3_parquet_folder',
        python_callable=delete_existing_parquet_files
    )

    glue_json_to_parquet = GlueJobOperator(
        task_id="glue_json_to_parquet",
        job_name="de6-team8-json-to-parquet",
        script_location="s3://de6-team8-bucket/glue/scripts/quasarzone/quasarzone_json_to_parquet.py",
        iam_role_name="de6-team8-glue-role",
        region_name="ap-northeast-2",
        wait_for_completion=True
    )

    glue_clean_parquet = GlueJobOperator(
        task_id="glue_clean_parquet",
        job_name="de6-team8-cleaning-job",
        script_location="s3://de6-team8-bucket/glue/scripts/quasarzone/quasarzone_parquet_cleaning.py",
        iam_role_name="de6-team8-glue-role",
        region_name="ap-northeast-2",
        wait_for_completion=True
    )

    copy_to_snowflake = SnowflakeOperator(
        task_id='copy_to_snowflake',
        sql="""
        DELETE FROM processed.quasarzone
        WHERE created_at = '{{ ds }}';

        COPY INTO processed.quasarzone
        FROM @quasarzone_stage/de6-team8-cleaning-job-{{ ds }}/
        FILE_FORMAT = (TYPE = PARQUET)
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        PATTERN = '.*\\.parquet$';
        """,
        snowflake_conn_id='team8_snowflake_conn',
    )

    [task_pc_hardware, task_notebook_mobile] >> clear_s3_parquet
    clear_s3_parquet >> glue_json_to_parquet >> glue_clean_parquet >> copy_to_snowflake
