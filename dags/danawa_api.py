from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import time, json, os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup

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

# 크롤링 및 S3 업로드
def run_danawa_crawl(**context):
    date_str = datetime.now().strftime('%Y-%m-%d')
    filename = f"danawa_{date_str}.json"
    os.makedirs(LOCAL_TMP_DIR, exist_ok=True)
    local_path = os.path.join(LOCAL_TMP_DIR, filename)
    s3_key = S3_KEY_TEMPLATE.format(date=date_str)

    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(options=options)

    records = []
    for url in SEARCH_SITE:
        driver.get(url)
        driver.implicitly_wait(1)
        # 인기순 정렬
        driver.find_element(
            By.CSS_SELECTOR,
            "div.prod_list_opts ul.order_list li.order_item[data-sort-method='BoardCount']"
        ).click()
        time.sleep(2)
        while True:
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            for prod in soup.select('div.prod_main_info'):
                item = {}
                # 제품명
                title_el = prod.select_one('p.prod_name a')
                if title_el and title_el.text.strip():
                    item['title'] = title_el.text.strip()
                # 출시일
                rel_el = prod.select_one('div.prod_sub_info dd')
                if rel_el and rel_el.text.strip():
                    item['release_date'] = rel_el.text.strip()[:-1]
                # 링크
                link_el = prod.select_one('div.thumb_image a')
                if link_el and link_el.get('href'):
                    item['link'] = link_el['href']
                # 가격 정보: 리스트로 수집 후 오름차순, 문자열로 저장
                price_items = prod.select('div.prod_pricelist li')
                prices = []
                for price_el in price_items:
                    price_tag = price_el.select_one('p.price_sect a')
                    if price_tag and price_tag.text.strip():
                        text = price_tag.text.replace(',','').replace('원','').strip()
                        if text.isdigit():
                            prices.append(int(text))
                if prices:
                    prices_sorted = sorted(prices)
                    item['price_info'] = ','.join(str(p) for p in prices_sorted)
                # 스펙: 실제 있는 key:value와 비콜론 없는 항목을 하나의 dict로 묶어 JSON 문자열로 저장
                spec_el = prod.select_one('div.spec_list')
                if spec_el and spec_el.text.strip():
                    parts = [p.strip() for p in spec_el.text.strip().split('/')]
                    combined = []
                    for part in parts:
                        if ':' in part:
                            combined.append(part)
                        else:
                            if combined and ':' in combined[-1]:
                                combined[-1] += '/' + part
                            else:
                                combined.append(part)
                    spec_dict = {}
                    # 비콜론 없는 항목
                    non_colon = [c for c in combined if ':' not in c]
                    for idx, val in enumerate(non_colon, start=1):
                        if val:
                            spec_dict[f'Spec_{idx}'] = val
                    # key:value 항목
                    for c in combined:
                        if ':' in c:
                            k, v = c.split(':', 1)
                            k, v = k.strip(), v.strip()
                            if v:
                                spec_dict[k] = v
                    if spec_dict:
                        item['spec'] = json.dumps(spec_dict, ensure_ascii=False)
                records.append(item)
            # 다음 페이지로 이동
            try:
                nav = driver.find_element(By.CSS_SELECTOR, '#productListArea .prod_num_nav')
                next_btn = nav.find_element(By.CSS_SELECTOR, 'a.pg_next')
                next_btn.click()
                time.sleep(2)
            except:
                break

    driver.quit()

    # JSON 파일로 저장
    with open(local_path, 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

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

with DAG(
    dag_id='danawa_crawl_and_clean',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['danawa', 'crawler', 'glue', 'snowflake'],
    max_active_runs=1,
) as dag:

    crawl_task = PythonOperator(
        task_id='crawl_and_upload_danawa',
        python_callable=run_danawa_crawl
    )

    glue_task = GlueJobOperator(
        task_id='glue_transform_danawa',
        job_name='danawa_json_to_parquet',
        script_location='s3://de6-team8-bucket/glue/scripts/danawa/danawa_json_to_parquet.py',
        iam_role_name='de6-team8-glue-role',
        region_name='ap-northeast-2',
        wait_for_completion=True
    )

    copy_to_snowflake = SnowflakeOperator(
        task_id='copy_to_snowflake',
        sql="""
            COPY INTO processed.danawa
            FROM @danawa_stage/date={{ ds }}/
            FILE_FORMAT = (TYPE = PARQUET)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            PATTERN = '.*\\.parquet$';
        """,
        snowflake_conn_id='team8_snowflake_conn'
    )

crawl_task >> glue_task >> copy_to_snowflake
