from airflow.sdk import dag
from include.pipelines.task_groups import extract_group, input_validation_group, load_group, output_validation_group, transform_group
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
    checked = input_validation_group(extracted)
    transformed = transform_group(checked)
    validated = output_validation_group(transformed)
    load_group(validated)


globo_retail_etl()

