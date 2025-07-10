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

def upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
    # S3Hook을 사용하여 AWS 연결 설정
    hook = S3Hook('aws_default')  # Airflow UI에서 설정한 AWS 연결 ID 사용
    
    # S3 업로드 경로 설정
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)
    print(f"✅ 파일 업로드 완료: {key}")

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

    # 1) 타임스탬프로 파일명 생성
    timestamp = datetime.now().strftime("%Y-%m-%d")
    filename = f"naver_{timestamp}.json"
    local_path = os.path.join("/tmp", filename)

    # 2) 로컬에 JSON 덤프
    with open(local_path, "w", encoding="utf-8") as f:
        json.dump(total, f, ensure_ascii=False, indent=2)
    print(f"✔️ 로컬 파일 생성 완료: {local_path}")

    # 3) S3에 업로드 (raw_data 하위)
    s3_key = f"raw_data/naver/{filename}"
    upload_to_s3(filename=local_path, key=s3_key, bucket_name="de6-team8-bucket")

default_args = {
    'start_date': datetime(2025, 7, 8),
    'retries': 3,
    'retry_delay': pd.Timedelta(minutes=1),
}

with DAG(
    dag_id='naverapi_scraper',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['naver','scraper']
) as dag:
    naver_shopping_task = PythonOperator(
        task_id='naver_shopping',
        python_callable=run_naver_shopping
    )

naver_shopping_task
