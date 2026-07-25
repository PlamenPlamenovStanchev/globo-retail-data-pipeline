from airflow.sdk import dag
from include.pipelines.retail_pipeline import load_processed_task, transform_retail_task, validate_output_task
from include.pipelines.task_groups import extract_group
from pendulum import datetime



@dag(
    dag_id="retail_dag",
    start_date=datetime(2026, 7, 23),
    schedule=None,
    catchup=False,
    tags=["retail", "s3", "etl"],
)
def globo_retail_etl():
    extracted = extract_group()
    transformed = transform_retail_task(extracted["sales"], extracted["products"])
    validated = validate_output_task(transformed["transformed"])
    load_processed_task(validated)


globo_retail_etl()

