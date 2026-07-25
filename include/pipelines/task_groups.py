"""Logical Airflow TaskGroups for the retail ETL pipeline stages."""

from typing import Any

from airflow.sdk import task_group

from include.pipelines.tasks import (
    extract_products_task,
    extract_sales_task,
    transform_retail_task,
    validate_products_input_task,
    validate_retail_output_task,
    validate_sales_input_task,
    write_processed_data_task,
)


@task_group(group_id="extract")
def extract_group() -> dict[str, Any]:
    """Instantiate parallel RAW sales and products extraction tasks."""
    return {
        "sales": extract_sales_task(),
        "products": extract_products_task(),
    }


@task_group(group_id="input_validation")
def input_validation_group(extracted: dict[str, Any]) -> dict[str, Any]:
    """Instantiate independent non-blocking validation tasks for extracted datasets."""
    return {
        "sales": validate_sales_input_task(extracted["sales"]),
        "products": validate_products_input_task(extracted["products"]),
    }


@task_group(group_id="transform")
def transform_group(validated_inputs: dict[str, Any]) -> Any:
    """Instantiate the cohesive transform/join task and return its metadata result."""
    return transform_retail_task(validated_inputs["sales"], validated_inputs["products"])


@task_group(group_id="output_validation")
def output_validation_group(transformation_result: Any) -> Any:
    """Instantiate the strict output gate for the transformed dataset reference."""
    return validate_retail_output_task(transformation_result["transformed"])


@task_group(group_id="load_processed")
def load_group(validated_output: Any) -> Any:
    """Instantiate processed-zone persistence for strictly approved output only."""
    return write_processed_data_task(validated_output)
