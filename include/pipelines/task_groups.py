"""Logical Airflow TaskGroups for the retail ETL pipeline."""

from typing import Any

from airflow.sdk import task_group

from include.pipelines.retail_pipeline import (
    extract_products_task, extract_sales_task, transform_products_task, transform_retail_task,
    transform_sales_task, validate_products_input_task, validate_sales_input_task,
)


@task_group(group_id="extract")
def extract_group() -> dict[str, Any]:
    """Instantiate parallel RAW extraction tasks."""
    return {
        "sales": extract_sales_task(),
        "products": extract_products_task(),
    }


@task_group(group_id="input_validation")
def input_validation_group(extracted: dict[str, Any]) -> dict[str, Any]:
    """Instantiate parallel non-blocking input-validation tasks."""
    return {"sales": validate_sales_input_task(extracted["sales"]), "products": validate_products_input_task(extracted["products"])}


@task_group(group_id="transform")
def transform_group(validated: dict[str, Any]) -> Any:
    """Instantiate parallel source transformations followed by their retail join."""
    sales = transform_sales_task(validated["sales"])
    products = transform_products_task(validated["products"])
    return transform_retail_task(sales, products)
