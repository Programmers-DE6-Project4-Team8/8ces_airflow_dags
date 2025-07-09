from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import urllib.request
import urllib.parse
import json
import time

client_id = "OV0nQqLPAApHItg6THZX"
client_secret = "Akl1n4D5Ka"

def search_naver_shopping(query, start=1, display=100, sort="sim"):
    enc = urllib.parse.quote(query)
    url = f"https://openapi.naver.com/v1/search/shop?query={enc}&sort={sort}&start={start}&display={display}"
    req = urllib.request.Request(url)
    req.add_header("X-Naver-Client-Id", client_id)
    req.add_header("X-Naver-Client-Secret", client_secret)
    res = urllib.request.urlopen(req)
    if res.getcode() == 200:
        return json.loads(res.read().decode('utf-8')).get('items', [])
    return []

def collect_shopping_data_by_category(category, target_count=1000):
    all_items = []
    seen = set()
    start = 1
    while len(all_items) < target_count and start <= 1000:
        items = search_naver_shopping(category, start, 100)
        new = [it for it in items if it.get('productId') not in seen]
        for it in new:
            seen.add(it.get('productId'))
        if not new:
            break
        all_items.extend(new)
        start += 100
        time.sleep(0.5)
    return all_items

def run_naver_shopping():
    categories = [
        "CPU", "GPU", "RAM", "SSD", "HDD",
        "메인보드", "파워서플라이",
        "사무용 노트북", "게이밍 노트북",
        "스마트폰", "태블릿", "이어폰"
    ]
    total = []
    for cat in categories:
        data = collect_shopping_data_by_category(cat, 1000)
        total.extend(data)
        time.sleep(1)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    with open(f"/tmp/naver_shopping_all_{timestamp}.json", "w", encoding="utf-8") as f:
        json.dump(total, f, ensure_ascii=False, indent=2)

default_args = {
    'start_date': datetime(2025, 7, 8),
    'retries': 1,
    'retry_delay': pd.Timedelta(minutes=1),
}

with DAG(
    dag_id='naverapi_scraper',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['naver','scraper']
) as dag:
    naver_shopping_task = PythonOperator(
        task_id='naver_shopping',
        python_callable=run_naver_shopping
    )
naver_shopping_task
