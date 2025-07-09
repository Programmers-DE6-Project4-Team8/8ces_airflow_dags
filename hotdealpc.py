from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup as bs
import json
import os
import re

# S3 업로드 작업을 위한 함수
def upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
    # S3Hook을 사용하여 AWS 연결 설정
    hook = S3Hook('aws_default')  # Airflow UI에서 설정한 AWS 연결 ID 사용
    
    # S3 업로드 경로 설정
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
    base_url = "https://quasarzone.com/bbs/qb_saleinfo?_method=post&type=&page={}&_token=6IHeiASojdKXgrQtsupUaDsRnhx6b7bLHh24pkda&category=%EB%85%B0%ED%8A%B8%EB%B6%81%2F%EB%AA%A8%EB%B0%94%EC%9D%BC&shop=&popularity=&kind=subject&keyword=&sort=num%2C+reply&direction=DESC"
    hotdeal_info = []
    page = 1

    while True:
        url = base_url.format(page)
        soup = get_soup(url)

        rows = soup.select("div.list-board-wrap div.market-type-list table tbody tr")
        if not rows:  # 더 이상 데이터가 없으면 종료
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

        page += 1  # 다음 페이지로 이동

    # 기존 JSON 파일 읽기 (이미 존재하는 데이터와 합치기)
    file_path = "/tmp/hotdeal_data.json"
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8') as f:
            existing_data = json.load(f)
    else:
        existing_data = []

    # 기존 데이터에 새로운 데이터 추가
    existing_data.extend(hotdeal_info)

    # 모든 데이터를 JSON으로 저장
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(existing_data, f, ensure_ascii=False, indent=4)
    print(f"✅ 저장 완료: {file_path}")

    # ✅ 로그에 최근 5개 데이터 출력
    print("✅ 최근 5개 데이터:")
    print(json.dumps(existing_data[-5:], ensure_ascii=False, indent=4))

    # S3 업로드 함수 호출
    upload_to_s3(filename=file_path, key='raw_data/quasarzone/hotdeal_data.json', bucket_name='de6-team8-bucket')

# DAG 기본 설정
default_args = {
    'start_date': datetime(2022, 1, 1),  # 2022년 1월 1일부터 시작
    'retries': 1,
    'retry_delay': timedelta(minutes=1)  # 판다스 없이 timedelta 사용
}

# DAG 정의
with DAG(
    dag_id='hotdeal_scraper_recent_data',
    default_args=default_args,
    schedule_interval='@daily',  # 하루에 한 번 실행
    catchup=False,
    tags=['hotdeal', 'scraper']
) as dag:

    scrape_task = PythonOperator(
        task_id='scrape_recent_data',
        python_callable=scrape_recent_data
    )

    scrape_task  # DAG 실행
