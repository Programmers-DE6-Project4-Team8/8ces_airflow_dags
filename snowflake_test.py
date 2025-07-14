from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

with DAG(
    dag_id="snowflake_test_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["test"],
) as dag:

    run_sql = SnowflakeOperator(
        task_id="test_snowflake_connection",
        sql="SELECT CURRENT_TIMESTAMP;",
        snowflake_conn_id="team8_snowflake_conn",
    )
