from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
import time
import re

# 기본 설정
default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

# 전역 연도/월 변수 (parse_date 에서 사용)
YEAR = datetime.today().year
MONTH = datetime.today().month

def get_soup_from_page_with(url):
    headers = {'User-Agent': 'Mozilla/5.0'}
    res = requests.get(url, headers=headers)
    return bs(res.text, 'lxml')

def is_empty_page(soup):
    return bool(soup.select_one("i.fa.fa-exclamation-triangle"))

def isBlocked(soup):
    return bool(soup.select_one("h2.title"))

def parse_views(views):
    if views.isdigit():
        return int(views)
    unit_map = {'k': 1_000, 'm': 1_000_000}
    return int(float(views[:-1]) * unit_map.get(views[-1].lower(), 1))

def parse_date(created_at):
    global YEAR, MONTH
    today = datetime.today().strftime('%Y-%m-%d')
    # "방금", "분 전", "HH:MM" 형식은 오늘 날짜
    if re.search(r'(방금|분 전|\d+분|\d+시간)', created_at) or re.match(r'\d{2}:\d{2}', created_at):
        return today
    # "MM-DD" 형식
    elif re.match(r'\d{2}-\d{2}', created_at):
        month = int(created_at[:2])
        # 1월에서 12월로 넘어간 경우 연도 보정
        if MONTH == 1 and month == 12:
            YEAR -= 1
        MONTH = month
        return f"{YEAR}-{created_at}"
    else:
        raise ValueError(f"알 수 없는 날짜 형식: {created_at}")

def extract_product_name_and_platform(title_text):
    # [플랫폼] 등을 추출
    platform_matches = re.findall(r"\[(.*?)\]", title_text)
    platform = ", ".join(platform_matches) if platform_matches else None
    # [](), () 안의 텍스트 제거하여 순수 제목만
    title_cleaned = re.sub(r"[\[\(].*?[\]\)]", "", title_text).strip()
    return title_cleaned, platform

def get_hotdeal_summary(hotdeal):
    votes   = hotdeal.select_one("td > span.num.num")
    title   = hotdeal.select_one("a.subject-link span.ellipsis-with-reply-cnt")
    price   = hotdeal.select_one("span.text-orange")
    views   = hotdeal.select_one("span.count")
    created = hotdeal.select_one("span.date")
    # 썸네일 이미지 태그에서 src 추출
    thumb   = hotdeal.select_one("div.thumb-wrap img")
    image   = thumb["src"].strip() if thumb and thumb.has_attr("src") else None

    if not all([votes, title, price, views, created]):
        return None

    return {
        "votes": votes.text.strip(),
        "title": title.text.strip(),
        "price": price.text.strip(),
        "views": parse_views(views.text.strip()),
        "created_at": parse_date(created.text.strip()),
        "category": None,   # 이후 scrap_hotdeal_info 에서 채워집니다
        "image": image
    }

def scrap_hotdeal_info(soup, hotdeal_info, category_name):
    rows = soup.select("table > tbody > tr")
    for hotdeal in rows:
        summary = get_hotdeal_summary(hotdeal)
        if summary:
            summary["category"] = category_name
            hotdeal_info.append(summary)

def crawl_quasarzone_category(category, base_url):
    today_str = datetime.today().strftime("%Y-%m-%d")
    until_date = datetime(2024, 1, 1)
    page = 1
    hotdeal_info = []
    global YEAR, MONTH
    YEAR = datetime.today().year
    MONTH = datetime.today().month

    while True:
        url = base_url.format(page)
        soup = get_soup_from_page_with(url)

        if is_empty_page(soup):
            break
        if isBlocked(soup):
            time.sleep(10)
            continue

        scrap_hotdeal_info(soup, hotdeal_info, category)
        # 마지막으로 수집한 항목의 날짜가 until_date 이전이면 중단
        last_date = datetime.strptime(hotdeal_info[-1]['created_at'], "%Y-%m-%d")
        if last_date < until_date:
            break

        page += 1
        time.sleep(1)

    # Pandas DataFrame 생성 및 필터링/정렬
    df = pd.DataFrame(hotdeal_info)
    df = df[df['created_at'] >= until_date.strftime('%Y-%m-%d')]
    df = df.sort_values(by="created_at")

    if not df.empty:
        # product_name, platform 컬럼 추가
        df['product_name'], df['platform'] = zip(*df['title'].apply(extract_product_name_and_platform))
        file_name = f"{today_str}.json"
        local_path = f"/tmp/{file_name}"
        # JSON 저장 (image 포함)
        df.to_json(local_path, orient="records", force_ascii=False, indent=2)

        # S3 업로드
        s3 = S3Hook(aws_conn_id='aws_default')
        s3_key = f"raw_data/quasarzone/{category}/{file_name}"
        s3.load_file(local_path, bucket_name="de6-team8-bucket", key=s3_key, replace=True)

with DAG(
    dag_id='quasarzone_v2_final',
    default_args=default_args,
    schedule_interval='0 9 * * *',
    start_date=datetime(2025, 7, 10),
    catchup=False,
    tags=['quasarzone', 'etl']
) as dag:

    task_pc_hardware = PythonOperator(
        task_id='crawl_pc_hardware',
        python_callable=crawl_quasarzone_category,
        op_args=[
            "pc_hardware",
            "https://quasarzone.com/bbs/qb_saleinfo?_method=post&_token=xxx&category=PC%2F%ED%95%98%EB%93%9C%EC%9B%A8%EC%96%B4&kind=subject&sort=num%2C+reply&direction=DESC&page={}"
        ]
    )

    task_notebook_mobile = PythonOperator(
        task_id='crawl_notebook_mobile',
        python_callable=crawl_quasarzone_category,
        op_args=[
            "notebook_mobile",
            "https://quasarzone.com/bbs/qb_saleinfo?_method=post&_token=xxx&category=%EB%85%B8%ED%8A%B8%EB%B6%81%2F%EB%AA%A8%EB%B0%94%EC%9D%BC&kind=subject&sort=num%2C+reply&direction=DESC&page={}"
        ]
    )

    glue_transform = GlueJobOperator(
        task_id="run_glue_job",
        job_name="de6-team8-testjob",
        script_location="s3://de6-team8-bucket/glue/scripts/quasarzone/quasarzone_json_to_parquet.py",
        iam_role_name="de6-team8-glue-role",
        region_name="ap-northeast-2",
        wait_for_completion=True
    )

    # ① 외부 테이블 생성
    create_ext_table = SnowflakeOperator(
        task_id='create_ext_table',
        snowflake_conn_id='team8_snowflake_conn',
        sql="""
        CREATE OR REPLACE EXTERNAL TABLE ext_quasarzone (
          votes        STRING AS ( VALUE:"votes"       ::STRING ),
          title        STRING AS ( VALUE:"title"       ::STRING ),
          price        STRING AS ( VALUE:"price"       ::STRING ),
          views        NUMBER AS ( VALUE:"views"       ::NUMBER ),
          created_at   DATE AS ( VALUE:"created_at"  ::DATE ),
          category     STRING AS ( VALUE:"category"    ::STRING ),
          product_name STRING AS ( VALUE:"product_name"::STRING ),
          platform     STRING AS ( VALUE:"platform"    ::STRING ),
          image        STRING AS ( VALUE:"image"       ::STRING )
        )
        WITH LOCATION = @quasarzone_stage/de6-team8-testjob-{{ ds }}/
        FILE_FORMAT = (TYPE = 'PARQUET')
        AUTO_REFRESH = FALSE;
        """
    )

    # ② 외부 테이블을 대상으로 MERGE 수행 (중복 방지)
    merge_to_snowflake = SnowflakeOperator(
        task_id='merge_to_snowflake',
        snowflake_conn_id='team8_snowflake_conn',
        sql="""
        MERGE INTO processed.quasarzone AS target
        USING ext_quasarzone AS source
          ON target.title = source.title
        WHEN NOT MATCHED THEN
          INSERT (
            votes, title, price, views, created_at,
            category, product_name, platform, image
          )
          VALUES (
            source.votes, source.title, source.price, source.views, source.created_at,
            source.category, source.product_name, source.platform, source.image
          );
        """
    )

    # 태스크 의존성 설정
    [task_pc_hardware, task_notebook_mobile] >> glue_transform >> create_ext_table >> merge_to_snowflake





