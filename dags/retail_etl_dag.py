from airflow.sdk import dag
from pendulum import datetime



@dag(
    dag_id="retail_dag",
    start_date=datetime(2026, 7, 23),
    schedule=None,
    catchup=False,
    tags=["retail", "s3", "etl"],
)

def retail_dag():
    pass