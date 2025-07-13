from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import requests, json, re, os, time
from bs4 import BeautifulSoup as bs

def get_soup(url):
    response = requests.get(url, headers={
        'User-Agent': 'Mozilla/5.0'
    })
    return bs(response.text, 'lxml')

def parse_views(views):
    if views.isdigit():
        return int(views)
    unit_map = {'k': 1_000, 'm': 1_000_000}
    return int(float(views[:-1]) * unit_map.get(views[-1].lower(), 1))

def parse_date(created_at):
    today = datetime.today()
    if re.match(r'\d{2}:\d{2}', created_at) or re.match(r'\d+시간 전', created_at):
        return today.strftime('%Y-%m-%d')
    elif re.match(r'\d{2}-\d{2}', created_at):
        month, day = map(int, created_at.split('-'))
        year = today.year
        if (month, day) > (today.month, today.day):
            year -= 1
        return f"{year}-{month:02d}-{day:02d}"
    raise ValueError(f"Unknown date format: {created_at}")

def should_continue(created_at):
    try:
        parsed = parse_date(created_at)
        post_date = datetime.strptime(parsed, "%Y-%m-%d")
        return post_date >= datetime(2022, 1, 1)
    except Exception as e:
        print(f"⚠️ 날짜 해석 실패: {created_at} → {e}")
        return False

def upload_to_s3(filename, key, bucket):
    hook = S3Hook(aws_conn_id='aws_default')
    hook.load_file(filename=filename, key=key, bucket_name=bucket, replace=True)
    print(f"✅ 업로드 완료: s3://{bucket}/{key}")

def scrape_and_upload():
    categories = {
        "pc_hardware": "https://quasarzone.com/bbs/qb_saleinfo?category=PC%2F%ED%95%98%EB%93%9C%EC%9B%A8%EC%96%B4&page={}",
        "notebook_mobile": "https://quasarzone.com/bbs/qb_saleinfo?category=%EB%85%B8%ED%8A%B8%EB%B6%81%2F%EB%AA%A8%EB%B0%94%EC%9D%BC&page={}"
    }

    all_data = []

    for category, url_template in categories.items():
        print(f"🟢 {category} 크롤링 시작")
        page = 1
        while True:
            url = url_template.format(page)
            soup = get_soup(url)
            rows = soup.select("div.list-board-wrap div.market-type-list table tbody tr")
            if not rows:
                print(f"⚠️ {category}: 페이지 {page}에 항목 없음. 중단.")
                break

            stop_flag = False
            for row in rows:
                try:
                    title = row.select_one("a.subject-link span.ellipsis-with-reply-cnt")
                    price = row.select_one("span.text-orange")
                    views = row.select_one("span.count")
                    votes = row.select_one("span.num.num")
                    date = row.select_one("span.date")

                    if not all([title, price, views, votes, date]):
                        continue

                    created_at_raw = date.text.strip()
                    if not should_continue(created_at_raw):
                        print(f"🛑 {category}: 오래된 게시글 {created_at_raw} 발견. 중단.")
                        stop_flag = True
                        break

                    all_data.append({
                        "category": category,
                        "title": title.text.strip(),
                        "created_at": parse_date(created_at_raw),
                        "price": price.text.strip(),
                        "views": parse_views(views.text.strip()),
                        "votes": votes.text.strip()
                    })

                except Exception as e:
                    print(f"⚠️ 파싱 오류: {e}")
                    continue

            if stop_flag:
                break
            print(f"✅ {category} - 페이지 {page} 완료")
            page += 1
            time.sleep(1)

    # 저장 및 업로드
    date_str = datetime.now().strftime("%Y-%m-%d")
    local_path = f"/tmp/hotdeal_combined_{date_str}.json"
    with open(local_path, "w", encoding="utf-8") as f:
        for item in all_data:
            json.dump(item, f, ensure_ascii=False)
            f.write("\n")

    print(f"✅ 로컬 저장 완료: {local_path}")
    s3_key = f"raw_data/quasarzone/hotdeal_combined_{date_str}.json"
    upload_to_s3(local_path, s3_key, "de6-team8-bucket")

# DAG 설정
default_args = {
    'start_date': datetime(2025, 7, 13),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id="hotdeal_combined_etl_final",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["hotdeal", "quasarzone", "glue"]
) as dag:

    scrape_task = PythonOperator(
        task_id="scrape_quasarzone_task",
        python_callable=scrape_and_upload
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
