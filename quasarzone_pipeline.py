from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import requests, json, re, os, time
from bs4 import BeautifulSoup as bs

# ✅ S3 업로드 함수
def upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
    hook = S3Hook('aws_default')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name, replace=True)
    print(f"S3 업로드 완료: {key}")

# ✅ BeautifulSoup 요청 함수
def get_soup(url):
    response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
    return bs(response.text, 'lxml')

# ✅ 날짜 파싱 함수 (2022-01-01 이전이면 None 반환)
def parse_date(created_at):
    today = datetime.today()
    cutoff = datetime(2022, 1, 1)
    time_pattern = r'\d{2}:\d{2}'
    date_pattern = r'\d{2}-\d{2}'

    if re.match(time_pattern, created_at):
        return today.strftime('%Y-%m-%d')
    elif re.match(date_pattern, created_at):
        month, day = map(int, created_at.split('-'))
        year = today.year
        post_date = datetime(year, month, day)
        if post_date < cutoff:
            return None
        return post_date.strftime('%Y-%m-%d')
    return None

# ✅ 조회수 파싱 함수
def parse_views(views):
    unit_map = {'k': 1000, 'm': 1_000_000}
    if views[-1].lower() in unit_map:
        return int(float(views[:-1]) * unit_map[views[-1].lower()])
    return int(views)

# ✅ 크롤링 및 저장 함수
def scrape_quasarzone():
    base_url = "https://quasarzone.com/bbs/qb_saleinfo?_method=post&_token=dummy&category=%EB%85%B8%ED%8A%B8%EB%B6%81%2F%EB%AA%A8%EB%B0%94%EC%9D%BC&kind=subject&sort=num%2C+reply&direction=DESC&page={}"
    data = []
    page = 1

    while True:
        soup = get_soup(base_url.format(page))
        rows = soup.select("div.list-board-wrap div.market-type-list table tbody tr")
        if not rows:
            break

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
                    print("🛑 2022-01-01 이전 데이터 도달. 종료")
                    return save_data(data)

                data.append({
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

    save_data(data)

# ✅ 저장 및 S3 업로드 함수
def save_data(data):
    date_str = datetime.now().strftime("%Y-%m-%d")
    filename = f"quasarzone-{date_str}.json"
    local_path = f"/home/ec2-user/tmp/{filename}"

    with open(local_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"✔️ 로컬 저장 완료: {local_path}")
    upload_to_s3(local_path, f"raw_data/quasarzone/{filename}", "de6-team8-bucket")

# ✅ DAG 정의
default_args = {
    "start_date": datetime(2025, 7, 13),
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

with DAG(
    dag_id="quasarzone_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["quasarzone", "scraper", "glue"]
) as dag:

    # 🔹 Task 1: 크롤링
    quasar_scraper = PythonOperator(
        task_id="quasar_scraper",
        python_callable=scrape_quasarzone
    )

    # 🔹 Task 2: Glue 변환
    quasar_transform = GlueJobOperator(
        task_id="quasar_transform",
        job_name="quasarzone_json_to_parquet",  # 콘솔에 등록된 Glue Job 이름
        script_location="s3://de6-team8-bucket/glue/scripts/quasarzone/quasarzone_json_to_parquet.py",
        iam_role_name="de6-team8-glue-role",
        region_name="ap-northeast-2",
        wait_for_completion=True
    )

    quasar_scraper >> quasar_transform
