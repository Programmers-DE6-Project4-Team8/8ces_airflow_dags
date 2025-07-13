from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup as bs
import json
import os
import re
import time

# S3 업로드
def upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
    hook = S3Hook('aws_default')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name, replace=True)
    print(f"S3 업로드 완료: {key}")

# 요청
def get_soup(url):
    response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
    return bs(response.text, 'lxml')

# 날짜 유효성 검사
def is_valid_date(created_at):
    today = datetime.today()
    cutoff = datetime(2022, 1, 1)
    if re.match(r'\d{2}:\d{2}', created_at) or re.match(r'\d+시간 전', created_at):
        return True
    elif re.match(r'\d{2}-\d{2}', created_at):
        month, day = map(int, created_at.split('-'))
        post_date = datetime(today.year, month, day)
        return post_date >= cutoff
    return False

# 날짜 파싱
def parse_created_at(created_at):
    today = datetime.today()
    if re.match(r'\d{2}:\d{2}', created_at) or re.match(r'\d+시간 전', created_at):
        return today.strftime('%Y-%m-%d')
    elif re.match(r'\d{2}-\d{2}', created_at):
        month, day = map(int, created_at.split('-'))
        return f"{today.year}-{month:02d}-{day:02d}"
    return None

# 조회수 파싱
def parse_views(views):
    if views.isdigit():
        return int(views)
    unit_map = {'k': 1_000, 'm': 1_000_000}
    return int(float(views[:-1]) * unit_map.get(views[-1].lower(), 1))

# 크롤링 메인 함수
def scrape_all_quasarzone():
    categories = {
        "pc_hardware": "https://quasarzone.com/bbs/qb_saleinfo?_method=post&_token=dummy&category=PC%2F%ED%95%98%EB%93%9C%EC%9B%A8%EC%96%B4&kind=subject&sort=num%2C+reply&direction=DESC&page={}",
        "notebook_mobile": "https://quasarzone.com/bbs/qb_saleinfo?_method=post&_token=dummy&category=%EB%85%B8%ED%8A%B8%EB%B6%81%2F%EB%AA%A8%EB%B0%94%EC%9D%BC&kind=subject&sort=num%2C+reply&direction=DESC&page={}"
    }

    all_data = []
    for category, url_template in categories.items():
        page = 1
        while True:
            soup = get_soup(url_template.format(page))
            rows = soup.select("div.list-board-wrap div.market-type-list table tbody tr")
            if not rows:
                break

            for row in rows:
                try:
                    title = row.select_one("a.subject-link span.ellipsis-with-reply-cnt")
                    price = row.select_one("span.text-orange")
                    views = row.select_one("span.count")
                    votes = row.select_one("span.num.num")
                    date = row.select_one("span.date")

                    if not all([title, price, views, votes, date]):
                        continue

                    created_at_raw = date.text.strip()
                    if not is_valid_date(created_at_raw):
                        print(f"{category}: 2022-01 이전 게시물 도달. 중단")
                        return save_and_upload(all_data)

                    created_at = parse_created_at(created_at_raw)

                    all_data.append({
                        "category": category,
                        "title": title.text.strip(),
                        "created_at": created_at,
                        "price": price.text.strip(),
                        "views": parse_views(views.text.strip()),
                        "votes": votes.text.strip()
                    })
                except Exception as e:
                    print(f"파싱 오류: {e}")
                    continue

            print(f"{category} - 페이지 {page} 완료")
            page += 1
            time.sleep(1)

    save_and_upload(all_data)

# 저장 및 S3 업로드
def save_and_upload(data):
    filename = f"hotdeal_combined_{datetime.now().strftime('%Y-%m-%d')}.json"
    local_path = os.path.join("/home/ec2-user/tmp", filename)

    with open(local_path, "w", encoding="utf-8") as f:
        for item in data:
            json.dump(item, f, ensure_ascii=False)
            f.write("\n")

    print(f"로컬 저장 완료: {local_path}")
    upload_to_s3(filename=local_path, key=f"raw_data/quasarzone/{filename}", bucket_name="de6-team8-bucket")

# DAG 정의
default_args = {
    "start_date": datetime(2025, 7, 13),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="hotdeal_combined_etl",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["hotdeal", "quasarzone", "glue"],
) as dag:

    scrape_task = PythonOperator(
        task_id="scrape_quasarzone_task",
        python_callable=scrape_all_quasarzone
    )

    glue_task = GlueJobOperator(
        task_id="glue_transform_task",
        job_name="quasarzone_json_to_parquet",
        script_location="s3://de6-team8-bucket/glue/scripts/quasarzone/quasarzone_json_to_parquet.py",
        iam_role_name="de6-team8-glue-role",
        region_name="ap-northeast-2",
        wait_for_completion=True
    )

    scrape_task >> glue_task
