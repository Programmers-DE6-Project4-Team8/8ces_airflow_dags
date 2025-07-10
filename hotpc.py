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

# (현재 미사용) 네이버 API용 변수
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
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36'
    })
    return bs(response.text, 'lxml')

# 조회수 처리
def parse_views(views):
    if views.isdigit():
        return int(views)
    unit_map = {'k': 1_000, 'm': 1_000_000, 'b': 1_000_000_000}
    unit = views[-1]
    return int(float(views[:-1]) * unit_map.get(unit, 1))

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
        if post_date < cutoff_date:
            return None
        return post_date.strftime('%Y-%m-%d')
    return None

# 페이지 차단 확인
def is_blocked(soup):
    return bool(soup.select_one("h2.title"))

# 빈 페이지 확인
def is_empty(soup):
    return bool(soup.select_one("i.fa.fa-exclamation-triangle"))

# 크롤링 함수
def scrape_recent_data():
    base_url = "https://quasarzone.com/bbs/qb_saleinfo?_method=post&type=&page={}&_token=6IHeiASojdKXgrQtsupUaDsRnhx6b7bLHh24pkda&category=%EB%85%B8%ED%8A%B8%EB%B6%81%2F%EB%AA%A8%EB%B0%94%EC%9D%BC&shop=&popularity=&kind=subject&keyword=&sort=num%2C+reply&direction=DESC"
    hotdeal_info = []
    page = 1

    while True:
        soup = get_soup(base_url.format(page))

        if is_blocked(soup):
            print("⚠️ 차단됨. 15초 대기 후 재시도...")
            time.sleep(15)
            continue
        if is_empty(soup):
            print("✅ 더 이상 페이지 없음. 종료")
            break

        rows = soup.select("div.list-board-wrap div.market-type-list table tbody tr")
        for row in rows:
            try:
                title = row.select_one("a.subject-link span.ellipsis-with-reply-cnt")
                price = row.select_one("span.text-orange")
                views = row.select_one("span.count")
                votes = row.select_one("span.num.num")
                date = row.select_one("span.date")

                if not all([title, price, views, votes, date]):
                    continue

                created_at = parse_date(date.text.strip())
                if not created_at:
                    print("🛑 2022-01-01 이전 글 발견. 크롤링 중단.")
                    return save_and_upload(hotdeal_info)

                hotdeal_info.append({
                    "title": title.text.strip(),
                    "created_at": created_at,
                    "price": price.text.strip(),
                    "views": parse_views(views.text.strip()),
                    "votes": votes.text.strip()
                })
            except Exception as e:
                print(f"⚠️ 파싱 에러: {e}")
                continue

        print(f"✅ 페이지 {page} 완료")
        page += 1
        time.sleep(1)

    save_and_upload(hotdeal_info)

# 저장 및 업로드 함수
def save_and_upload(data):
    timestamp = datetime.now().strftime("%Y-%m-%d")
    filename = f"hotdeal_{timestamp}.json"
    local_path = os.path.join("/home/ec2-user/tmp", filename)

    with open(local_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"✔️ 로컬 파일 생성 완료: {local_path}")
    print(f"📦 총 수집 건수: {len(data)}")
    print(f"📑 샘플 데이터: {data[:3]}")

    s3_key = f"raw_data/quasarzone/{filename}"
    upload_to_s3(filename=local_path, key=s3_key, bucket_name="de6-team8-bucket")

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

    scrape_task  # 명시적으로 DAG에 등록
