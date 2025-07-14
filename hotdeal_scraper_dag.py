from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import re

def get_soup(url):
    response = requests.get(url, headers={
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36'
    })
    return bs(response.text, 'lxml')

def parse_views(views):
    if views.isdigit():
        return views
    unit_map = {'k': 1_000, 'm': 1_000_000, 'b': 1_000_000_000}
    unit = views[-1]
    views = float(views[:-1])
    return int(views * unit_map[unit])

def parse_date(created_at):
    today = datetime.today()
    time_pattern = r'\d{2}:\d{2}'
    date_pattern = r'\d{2}-\d{2}'
    hours_ago_pattern = r'\d+시간 전'

    if re.match(time_pattern, created_at) or re.match(hours_ago_pattern, created_at):
        return today.strftime('%Y-%m-%d')
    elif re.match(date_pattern, created_at):
        month, day = map(int, created_at.split('-'))
        year = today.year
        if today.month == 1 and month == 12:
            year -= 1
        return f"{year}-{month:02d}-{day:02d}"
    else:
        raise ValueError(f"Unknown date format: {created_at}")

def scrape_first_page():
    url = "https://quasarzone.com/bbs/qb_saleinfo?_method=post&type=&page=1&_token=6IHeiASojdKXgrQtsupUaDsRnhx6b7bLHh24pkda&category=%EB%85%B8%ED%8A%B8%EB%B6%81%2F%EB%AA%A8%EB%B0%94%EC%9D%BC&shop=&popularity=&kind=subject&keyword=&sort=num%2C+reply&direction=DESC"
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
    file_path = "/tmp/hotdeal_first_page.csv"
    hotdeal_info.to_csv(file_path, index=False, encoding='utf-8')
    print(f"✅ 저장 완료: {file_path}")

    # ✅ 로그에 5개 행만 출력
    print("✅ 상위 5개 데이터:")
    print(hotdeal_info.head(5).to_string(index=False))

# DAG 기본 설정
default_args = {
    'start_date': datetime(2025, 7, 8),
    'retries': 1,
    'retry_delay': pd.Timedelta(minutes=1)
}

with DAG(
    dag_id='hotdeal_scraper_first_page',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['hotdeal', 'scraper']
) as dag:

    scrape_task = PythonOperator(
        task_id='scrape_first_page',
        python_callable=scrape_first_page
    )

    scrape_task
