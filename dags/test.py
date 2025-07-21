from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_hello():
    print(" Hello DAG!")

with DAG(
    dag_id='dummy_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['test'],
    description='Test dummy DAG for S3-Lambda sync'
) as dag:

    hello_task = PythonOperator(
        task_id='say_hello',
        python_callable=print_hello
    )
