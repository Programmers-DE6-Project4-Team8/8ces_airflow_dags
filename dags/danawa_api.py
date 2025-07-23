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
import logging

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
    # 로그 포맷 설정 (Airflow 가 이미 세팅해 주지만, 추가 설정이 필요하다면 uncomment)
    # logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

    date_str = datetime.now().strftime('%Y-%m-%d')
    filename = f"danawa_{date_str}.json"
    os.makedirs(LOCAL_TMP_DIR, exist_ok=True)
    local_path = os.path.join(LOCAL_TMP_DIR, filename)
    s3_key = S3_KEY_TEMPLATE.format(date=date_str)

    logging.info(f"크롤 시작: 날짜={date_str}, 저장파일={local_path}")

    # Selenium WebDriver 초기화
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(options=options)

    records = []
    total_sites = len(SEARCH_SITE)

    for idx, url in enumerate(SEARCH_SITE, start=1):
        logging.info(f"[사이트 {idx}/{total_sites}] URL: {url} 크롤링 시작")
        driver.get(url)
        driver.implicitly_wait(1)

        # 인기순 정렬 클릭
        try:
            driver.find_element(
                By.CSS_SELECTOR,
                "div.prod_list_opts ul.order_list li.order_item[data-sort-method='BoardCount']"
            ).click()
            time.sleep(2)
            logging.info(f"[사이트 {idx}/{total_sites}] 인기순 정렬 적용 완료")
        except Exception as e:
            logging.warning(f"[사이트 {idx}/{total_sites}] 인기순 정렬 실패: {e}")

        page_num = 1
        while page_num <= 10:
            logging.info(f"[사이트 {idx}/{total_sites}] 페이지 {page_num} 스크래핑 시작")
            # soup = BeautifulSoup(driver.page_source, 'html.parser')
            # count_before = len(records)
            
            # 1) 페이지 끝까지 스크롤 다운
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1)

            soup = BeautifulSoup(driver.page_source, 'html.parser')
            count_before = len(records)
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
                # 가격 정보
                price_items = prod.select('div.prod_pricelist li')
                prices = []
                for price_el in price_items:
                    price_tag = price_el.select_one('p.price_sect a')
                    if price_tag and price_tag.text.strip():
                        text = price_tag.text.replace(',', '').replace('원', '').strip()
                        if text.isdigit():
                            prices.append(int(text))
                if prices:
                    records_sorted = sorted(prices)
                    item['price_info'] = ','.join(str(p) for p in records_sorted)
                # 스펙 정보
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
                    non_colon = [c for c in combined if ':' not in c]
                    for sidx, val in enumerate(non_colon, start=1):
                        if val:
                            spec_dict[f'Spec_{sidx}'] = val
                    for c in combined:
                        if ':' in c:
                            k, v = c.split(':', 1)
                            if v.strip():
                                spec_dict[k.strip()] = v.strip()
                    if spec_dict:
                        item['spec'] = json.dumps(spec_dict, ensure_ascii=False)
                img_el = prod.select_one('div.thumb_image img')
                if img_el and img_el.get('src'):
                    img_url = img_el['src']
                    # protocol-relative URL 보정
                    if img_url.startswith("//"):
                        img_url = "https:" + img_url
                    item['image_link'] = img_url
                    # logging.info(f"{img_url}")
                records.append(item)

            count_after = len(records)
            logging.info(f"[사이트 {idx}/{total_sites}] 페이지 {page_num} 완료: 누적 레코드 {count_after}개 (+{count_after - count_before})")

            # 종료 조건
            if page_num == 10:
                break

            page_num += 1
            time.sleep(2)
            # 페이지 네비게이션
            try:
                if page_num % 10 == 1:
                    driver.find_element(By.CSS_SELECTOR,
                        '#productListArea > div.prod_num_nav > div > a'
                    ).click()
                else:
                    page_nums = driver.find_element(
                        By.XPATH, '//*[@id="productListArea"]/div[4]/div'
                    )
                    page_nums.find_element(By.LINK_TEXT, str(page_num)).click()
                logging.info(f"[사이트 {idx}/{total_sites}] 페이지 {page_num} 네비게이션 완료")
            except Exception as e:
                logging.warning(f"[사이트 {idx}/{total_sites}] 페이지 {page_num} 네비게이션 실패: {e}")
                break

        logging.info(f"[사이트 {idx}/{total_sites}] URL: {url} 크롤링 완료")

    driver.quit()
    logging.info(f"크롤러 종료: 최종 누적 레코드 {len(records)}개")

    # JSON 파일로 저장
    with open(local_path, 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    logging.info(f"로컬 파일 저장 완료: {local_path}")

    # S3 업로드
    hook = S3Hook(aws_conn_id='aws_default')
    hook.load_file(
        filename=local_path,
        key=s3_key,
        bucket_name=BUCKET_NAME,
        replace=True
    )
    logging.info(f"S3 업로드 완료: s3://{BUCKET_NAME}/{s3_key}")
# DAG 정의
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 17),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='danawa_etl_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['danawa', 'crawler', 'glue', 'snowflake'],
    max_active_runs=1,
) as dag:

    # crawl_task = PythonOperator(
    #     task_id='crawl_and_upload_danawa',
    #     python_callable=run_danawa_crawl
    # )

    # glue_task = GlueJobOperator(
    #     task_id='glue_transform_danawa',
    #     job_name='danawa_json_to_parquet',
    #     script_location='s3://de6-team8-bucket/glue/scripts/danawa/danawa_json_to_parquet.py',
    #     iam_role_name='de6-team8-glue-role',
    #     region_name='ap-northeast-2',
    #     wait_for_completion=True
    # )
    
    # create_ext_table = SnowflakeOperator(
    #     task_id='create_ext_table',
    #     snowflake_conn_id='team8_snowflake_conn',
    #     sql="""
    #     CREATE OR REPLACE EXTERNAL TABLE ext_danawa_shopping (
    #       title        STRING AS ( VALUE:"title"       ::STRING ),
    #       link         STRING AS ( VALUE:"link"        ::STRING ),
    #       release_date        STRING AS ( VALUE:"release_date"       ::STRING ),
    #       spec       STRING AS ( VALUE:"spec"      ::STRING ),
    #       price_info       STRING AS ( VALUE:"price_info"      ::STRING ),
    #       image_link       STRING AS ( VALUE:"image_link"          ::STRING)
    #     )
    #     WITH LOCATION = @danawa_stage/date={{ ds }}/
    #     FILE_FORMAT = (TYPE = 'PARQUET')
    #     AUTO_REFRESH = FALSE;
    #     """
    # )

    merge_to_snowflake = SnowflakeOperator(
        task_id='merge_to_snowflake',
        snowflake_conn_id='team8_snowflake_conn',
        sql="""
        MERGE INTO processed.danawa AS target
        USING ext_danawa_shopping AS source
          ON target.title = source.title
        WHEN NOT MATCHED THEN
          INSERT (
            title, link, release_date, spec, price_info, image_link
          )
          VALUES (
            source.title, source.link, source.release_date,
            source.spec, source.price_info, source.image_link
          );
        """
    )

# crawl_task >> glue_task >> create_ext_table >> merge_to_snowflake
merge_to_snowflake
