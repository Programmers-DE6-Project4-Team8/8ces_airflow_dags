from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup as bs
import json
import os
import re
import time

# 네이버 API용 (현재 사용은 안 함, 필요 시 확장 가능)
client_id = "OV0nQqLPAApHItg6THZX"
client_secret = "Akl1n4D5Ka"

# S3 업로드 함수
def upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
    hook = S3Hook('aws_default')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)
    print(f"✅ 파일 업로드 완료: {key}")

# BeautifulSoup 객체 생성
def get_soup(url):
    response = requests.get(url, headers={
        'User-Agent': 'Mozilla/5.0'
    })
    return bs(response.text, 'lxml')

# 조회수 단위 처리
def parse_views(views):
    if views.isdigit():
        return int(views)
    unit_map = {'k': 1_000, 'm': 1_000_000, 'b': 1_000_000_000}
    unit = views[-1]
    views = float(views[:-1])
    return int(views * unit_map[unit])

# 날짜 처리
def parse_date(created_at):
    today = datetime.today()
    cutoff_date = datetime(2022, 1, 1)
    time_pattern = r'\d{2}:\d{2}'
    date_pattern = r'\d{2}-\d{2}'
    hours_ago_pattern = r'\d+시간 전'

    if re.match(time_pattern, created_at) or re.match(hours_ago_pattern, created_at):
        return today.strftime('%Y-%m-%d')
    elif re.match(date_pattern, created_at):
        month, day = map(int, created_at.split('-'))
        year = today.year
        post_date = datetime(year, month, day)
        if post_date >= cutoff_date:
            return f"{year}-{month:02d}-{day:02d}"
        else:
            return None
    else:
        raise ValueError(f"Unknown date format: {created_at}")

# 크롤링 & 저장 함수
def scrape_recent_data():
    base_url = "https://quasarzone.com/bbs/qb_saleinfo?_method=post&type=&page={}&_token=6IHeiASojdKXgrQtsupUaDsRnhx6b7bLHh24pkda&category=%EB%85%B0%ED%8A%B8%EB%B6%81%2F%EB%AA%A8%EB%B0%94%EC%9D%BC&shop=&popularity=&kind=subject&keyword=&sort=num%2C+reply&direction=DESC"
    hotdeal_info = []
    page = 1

    while True:
        soup = get_soup(base_url.format(page))
        rows = soup.select("div.list-board-wrap div.market-type-list table tbody tr")
        if not rows:
            break

        for row in rows:
            try:
                votes = row.select_one("span.num.num")
                title = row.select_one("a.subject-link span.ellipsis-with-reply-cnt")
                price = row.select_one("span.text-orange")
                views = row.select_one("span.count")
                date = row.select_one("span.date")

                if not all([votes, title, price, views, date]):
                    continue

                created_at = parse_date(date.text.strip())
                if created_at is None:
                    continue

                hotdeal_info.append({
                    'title': title.text.strip(),
                    'created_at': created_at,
                    'price': price.text.strip(),
                    'views': parse_views(views.text.strip()),
                    'votes': votes.text.strip()
                })
            except Exception as e:
                print(f"⚠️ Error parsing row: {e}")
                continue

        page += 1

    # 로컬 파일로 저장
    timestamp = datetime.now().strftime("%Y-%m-%d")
    filename = f"hotdeal_{timestamp}.json"
    local_path = os.path.join("/home/ec2-user/tmp", filename)

    with open(local_path, 'w', encoding='utf-8') as f:
        json.dump(hotdeal_info, f, ensure_ascii=False, indent=2)

    print(f"✅ 로컬 저장 완료: {local_path}")

    # S3 업로드
    s3_key = f"raw_data/quasarzone/{filename}"
    upload_to_s3(filename=local_path, key=s3_key, bucket_name='de6-team8-bucket')

# DAG 설정
default_args = {
    'start_date': datetime(2025, 7, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='hotdeal_scraper_recent_data',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['hotdeal', 'scraper']
) as dag:
    scrape_task = PythonOperator(
        task_id='scrape_recent_data_task',
        python_callable=scrape_recent_data
    )

scrape_task
