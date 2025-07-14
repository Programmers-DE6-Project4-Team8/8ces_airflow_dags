from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="hello_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["example"]
) as dag:
    hello = BashOperator(
        task_id="say_hello",
        bash_command="echo Hello, Airflow from GitHub Actions!"
    )
