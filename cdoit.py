from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import re
import os

# S3 업로드 작업을 위한 함수
def upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
    # S3Hook을 사용하여 AWS 연결 설정
    hook = S3Hook('aws_default')  # Airflow UI에서 설정한 AWS 연결 ID 사용
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)
    print(f"✅ 파일 업로드 완료: {key}")

# 웹 페이지에서 데이터 추출
def get_soup(url):
    response = requests.get(url, headers={
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36'
    })
    return bs(response.text, 'lxml')

# 조회수 단위 처리
def parse_views(views):
    if views.isdigit():
        return views
    unit_map = {'k': 1_000, 'm': 1_000_000, 'b': 1_000_000_000}
    unit = views[-1]
    views = float(views[:-1])
    return int(views * unit_map[unit])

# 날짜 필터링 (최신 데이터부터 2022년 1월 1일까지만 수집)
def parse_date(created_at):
    today = datetime.today()
    cutoff_date = datetime(2022, 1, 1)  # 2022년 1월 1일 이후만 수집
    time_pattern = r'\d{2}:\d{2}'
    date_pattern = r'\d{2}-\d{2}'
    hours_ago_pattern = r'\d+시간 전'

    if re.match(time_pattern, created_at) or re.match(hours_ago_pattern, created_at):
        return today.strftime('%Y-%m-%d')  # 오늘 날짜로 반환
    elif re.match(date_pattern, created_at):
        month, day = map(int, created_at.split('-'))
        year = today.year
        post_date = datetime(year, month, day)
        if post_date >= cutoff_date:
            return f"{year}-{month:02d}-{day:02d}"  # 2022년 1월 1일 이후 날짜만 반환
        else:
            return None  # 2022년 1월 1일 이전 날짜는 무시
    else:
        raise ValueError(f"Unknown date format: {created_at}")

# 웹 스크래핑 작업 (최신 데이터부터 2022년까지 수집)
def scrape_recent_data():
    url = "https://quasarzone.com/bbs/qb_saleinfo?_method=post&type=&page=1&_token=6IHeiASojdKXgrQtsupUaDsRnhx6b7bLHh24pkda&category=%EB%85%B0%ED%8A%B8%EB%B6%81%2F%EB%AA%A8%EB%B0%94%EC%9D%BC&shop=&popularity=&kind=subject&keyword=&sort=num%2C+reply&direction=DESC"
    soup = get_soup(url)

    hotdeal_info = pd.DataFrame(columns=['title', 'created_at', 'price', 'views', 'votes'])

    for row in soup.select("div.list-board-wrap div.market-type-list table tbody tr"):
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

            hotdeal_info.loc[len(hotdeal_info)] = [
                title.text.strip(),
                parse_date(date.text.strip()),
                price.text.strip(),
                parse_views(views.text.strip()),
                votes.text.strip()
            ]
        except Exception as e:
            print(f"⚠️ Error parsing row: {e}")
            continue

    # ✅ CSV로 저장
    file_path = "/tmp/hotdeal_data.json"
    hotdeal_info.to_json(file_path, orient="records", lines=True, force_ascii=False)
    print(f"✅ 저장 완료: {file_path}")

    # ✅ 로그에 5개 행만 출력
    print("✅ 상위 5개 데이터:")
    print(hotdeal_info.head(5).to_string(index=False))

    # S3에 업로드할 경로 설정
    s3_key = 'test/hotdeal_data.json'  # test 폴더에 업로드
    upload_to_s3(file_path, s3_key, 'de6-team8-bucket')

# DAG 기본 설정
default_args = {
    'start_date': datetime(2022, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# DAG 정의
with DAG(
    dag_id='hotdeal_scraper_recent_data_v2',  # DAG ID 수정
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['hotdeal', 'scraper']
) as dag:

    scrape_task = PythonOperator(
        task_id='scrape_recent_data',
        python_callable=scrape_recent_data
    )

    scrape_task
