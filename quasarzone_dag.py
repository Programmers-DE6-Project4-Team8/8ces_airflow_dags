import requests, json, re, time, os
from bs4 import BeautifulSoup as bs
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import timedelta

def upload_to_s3(filename: str, key: str, bucket_name: str):
    hook = S3Hook('aws_default')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name, replace=True)
    print(f"S3 업로드 완료: {key}")

def get_soup(url):
    res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
    return bs(res.text, 'lxml')

def parse_date(created_at):
    today = datetime.today()
    cutoff = datetime(2022, 1, 1)
    if re.match(r'\d{2}:\d{2}', created_at) or re.match(r'\d+시간 전', created_at):
        return today.strftime('%Y-%m-%d')
    elif re.match(r'\d{2}-\d{2}', created_at):
        month, day = map(int, created_at.split('-'))
        post_date = datetime(today.year, month, day)
        if post_date < cutoff:
            return None
        return post_date.strftime('%Y-%m-%d')
    return None

def parse_views(views):
    unit_map = {'k': 1000, 'm': 1_000_000}
    if views[-1].lower() in unit_map:
        return int(float(views[:-1]) * unit_map[views[-1].lower()])
    return int(views)

def scrape_all_categories():
    categories = {
        "notebook_mobile": "https://quasarzone.com/bbs/qb_saleinfo?_method=post&type=&page={}&_token=dummy&category=%EB%85%B8%ED%8A%B8%EB%B6%81%2F%EB%AA%A8%EB%B0%94%EC%9D%BC&kind=subject&sort=num%2C+reply&direction=DESC",
        "pc_hardware": "https://quasarzone.com/bbs/qb_saleinfo?_method=post&type=&page={}&_token=dummy&category=PC%2F%ED%95%98%EB%93%9C%EC%9B%A8%EC%96%B4&kind=subject&sort=num%2C+reply&direction=DESC"
    }

    all_data = []

    for name, url_template in categories.items():
        page = 1
        category_data = []
        stop_flag = False

        while True:
            soup = get_soup(url_template.format(page))
            rows = soup.select("div.list-board-wrap div.market-type-list table tbody tr")
            if not rows:
                print(f"{name}: 더 이상 게시물 없음. 종료")
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
                        print(f"{name}: 2022년 1월 이전 게시물 발견. 해당 카테고리 중단.")
                        stop_flag = True
                        break

                    category_data.append({
                        "category": name,
                        "title": title.text.strip(),
                        "created_at": created_at,
                        "price": price.text.strip(),
                        "views": parse_views(views.text.strip()),
                        "votes": votes.text.strip()
                    })
                except Exception as e:
                    print(f"{name} 파싱 에러: {e}")
                    continue

            if stop_flag:
                break

            print(f"{name}: 페이지 {page} 완료")
            page += 1
            time.sleep(1)

        all_data.extend(category_data)

    save_and_upload(all_data)

def save_and_upload(data):
    date_str = datetime.now().strftime("%Y-%m-%d")
    filename = f"quasarzone_hotdeal_{date_str}.json"
    local_path = f"/home/ec2-user/tmp/{filename}"

    with open(local_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    s3_key = f"raw_data/quasarzone/{filename}"
    upload_to_s3(local_path, s3_key, "de6-team8-bucket")
    print(f"로컬 및 S3 저장 완료: {filename}")

default_args = {
    'start_date': datetime(2025, 7, 13),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id="quasarzone_hotdeal_etl",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["quasarzone", "hotdeal", "etl"]
) as dag:

    scrape_task = PythonOperator(
        task_id="scrape_hotdeal_task",
        python_callable=scrape_all_categories
    )

    glue_task = GlueJobOperator(
        task_id="glue_transform_task",
        job_name="quasarzone_json_to_parquet",
        script_location="s3://de6-team8-bucket/glue/scripts/quasarzone/quasarzone_json_to_parquet.py",
        iam_role_name="de6-team8-glue-role",
        region_name="ap-northeast-2",
        wait_for_completion=True
    )

    scrape_task >> glue_task
