from airflow.sdk import dag
from include.pipelines.retail_pipeline import load_processed_task, validate_output_task
from include.pipelines.task_groups import extract_group, input_validation_group, transform_group
from pendulum import datetime



@dag(
    dag_id="globo_retail_etl",
    start_date=datetime(2026, 7, 23),
    schedule=None,
    catchup=False,
    tags=["retail", "s3", "etl"],
)
def globo_retail_etl():
    extracted = extract_group()
    input_validated = input_validation_group(extracted)
    transformed = transform_group(input_validated)
    validated = validate_output_task(transformed)
    load_processed_task(validated)


globo_retail_etl()

