from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import time, json, os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup
import pandas as pd

# 설정
SEARCH_SITE = [
    "https://prod.danawa.com/list/?cate=11236451",
    "https://prod.danawa.com/list/?cate=11236452",
    "https://prod.danawa.com/list/?cate=11252476",
    "https://prod.danawa.com/list/?cate=112747",
    "https://prod.danawa.com/list/?cate=112753",
    "https://prod.danawa.com/list/?cate=112752",
    "https://prod.danawa.com/list/?cate=112760",
    "https://prod.danawa.com/list/?cate=112763",
    "https://prod.danawa.com/list/?cate=112751",
    "https://prod.danawa.com/list/?cate=112777",
    "https://prod.danawa.com/list/?cate=122515",
    "https://prod.danawa.com/list/?cate=12210596",
    "https://prod.danawa.com/list/?cate=12237349"
]
BUCKET_NAME = 'de6-team8-bucket'
S3_KEY_TEMPLATE = 'raw_data/danawa/danawa_{date}.json'
LOCAL_TMP_DIR = '/home/ec2-user/tmp'

# spec 분리 유틸
def parse_spec(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, row in df.iterrows():
        spec = str(row.get('spec', ''))
        parts = [p.strip() for p in spec.split('/')]
        combined = []
        for part in parts:
            if ':' in part:
                combined.append(part)
            else:
                if combined and ':' in combined[-1]:
                    combined[-1] += '/' + part
                else:
                    combined.append(part)
        data = row.to_dict()
        # 비콜론 항목
        non_colon = [c for c in combined if ':' not in c]
        for i, v in enumerate(non_colon, 1):
            data[f'Spec_{i}'] = v
        # 콜론 기반 키:값 분리
        for c in combined:
            if ':' in c:
                k, v = c.split(':', 1)
                data[k.strip()] = v.strip()
        rows.append(data)
    return pd.DataFrame(rows)

# 크롤링 및 S3 업로드
def run_danawa_crawl(**context):
    # 날짜별 파일명
    date_str = datetime.now().strftime('%Y-%m-%d')
    filename = f"danawa_{date_str}.json"
    local_path = os.path.join(LOCAL_TMP_DIR, filename)
    s3_key = S3_KEY_TEMPLATE.format(date=date_str)

    # Selenium 설정
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    # options.add_argument('--window-size=1920,1080')
    service = Service(executable_path="/usr/local/bin/chromedriver")
    driver = webdriver.Chrome(options=options)

    items = []
    for url in SEARCH_SITE:
        driver.get(url)
        driver.implicitly_wait(1)
        # 인기순 정렬
        driver.find_element(By.CSS_SELECTOR,
            "div.prod_list_opts ul.order_list li.order_item[data-sort-method='BoardCount']").click()
        time.sleep(2)
        page = 1
        while True:
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            for prod in soup.select('div.prod_main_info'):
                items.append({
                    'productModel': prod.select_one('p.prod_name a').get_text(strip=True) if prod.select_one('p.prod_name a') else '',
                    'release_date': prod.select_one('div.prod_sub_info dd').get_text(strip=True)[:-1] if prod.select_one('div.prod_sub_info dd') else '',
                    'link': prod.select_one('div.thumb_image a')['href'] if prod.select_one('div.thumb_image a') else '',
                    'spec': prod.select_one('div.spec_list').get_text(strip=True) if prod.select_one('div.spec_list') else ''
                })
            try:
                nav = driver.find_element(By.CSS_SELECTOR, '#productListArea .prod_num_nav')
                page += 1
                if page % 10 == 1:
                    nav.find_element(By.CSS_SELECTOR, 'a.pg_next').click()
                else:
                    nav.find_element(By.LINK_TEXT, str(page)).click()
                time.sleep(2)
            except:
                break

    driver.quit()

    # DataFrame 변환 및 spec 분리
    df = pd.DataFrame(items)
    df = parse_spec(df)
    if 'spec' in df.columns:
        df = df.drop(columns=['spec'])
    df.to_json(local_path, orient='records', force_ascii=False, indent=2)

    # S3 업로드
    hook = S3Hook(aws_conn_id='aws_default')
    hook.load_file(
        filename=local_path,
        key=s3_key,
        bucket_name=BUCKET_NAME,
        replace=True
    )
    print(f"✅ Danawa data uploaded to s3://{BUCKET_NAME}/{s3_key}")

# DAG 정의
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 17),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='danawa_crawl_and_transform',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['danawa', 'crawler']
)

crawl_task = PythonOperator(
    task_id='crawl_and_upload_danawa',
    python_callable=run_danawa_crawl,
    dag=dag
)

glue_task = GlueJobOperator(
    task_id='glue_transform_danawa',
    job_name='danawa_json_to_parquet',
    script_location='s3://de6-team8-bucket/glue/scripts/danawa/danawa_json_to_parquet.py',
    iam_role_name='de6-team8-glue-role',
    region_name='ap-northeast-2',
    wait_for_completion=True,
    dag=dag
)

crawl_task >> glue_task
