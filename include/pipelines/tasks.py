"""Backward-compatible imports for the moved Airflow task wrappers."""

from include.pipelines.retail_pipeline import (
    S3DatasetReference,
    _read_intermediate_dataframe,
    _task_run_identity,
    _write_intermediate_dataframe,
    extract_validate_products_task,
    extract_validate_sales_task,
    load_processed_task,
    transform_retail_task,
    validate_output_task,
)

__all__ = [
    "S3DatasetReference",
    "_read_intermediate_dataframe",
    "_task_run_identity",
    "_write_intermediate_dataframe",
    "extract_validate_products_task",
    "extract_validate_sales_task",
    "load_processed_task",
    "transform_retail_task",
    "validate_output_task",
]
