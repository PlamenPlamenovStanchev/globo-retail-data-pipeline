"""Logical Airflow TaskGroups for the retail ETL pipeline."""

from typing import Any

from airflow.sdk import task_group

from include.pipelines.retail_pipeline import extract_validate_products_task, extract_validate_sales_task


@task_group(group_id="extract")
def extract_group() -> dict[str, Any]:
    """Instantiate parallel RAW extraction and non-blocking validation tasks."""
    return {
        "sales": extract_validate_sales_task(),
        "products": extract_validate_products_task(),
    }
