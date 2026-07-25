"""Airflow TaskFlow wrappers that exchange S3 references rather than DataFrames."""

from dataclasses import asdict
from datetime import date, datetime, timedelta
import re
from typing import Any, TypedDict

from airflow.sdk import get_current_context, task
import pandas as pd

from include.etl.extract_data.products_extractor import extract_products
from include.etl.extract_data.sales_extractor import extract_sales
from include.etl.load_data.processed_s3_loader import write_processed_data
from include.etl.load_data.rejected_s3_loader import write_rejected_records
from include.etl.transform_data.retail_transformer import transform_retail
from include.utils.config_loader import load_config
from include.utils.logger import setup_logger
from include.utils.s3_utils import get_storage_options
from include.validations.input_validator import validate_products_input, validate_sales_input
from include.validations.output_validator import validate_retail_output


logger = setup_logger(__name__)
RETRY_DELAY = timedelta(seconds=30)


class S3DatasetReference(TypedDict):
    """JSON-serializable dataset metadata passed through Airflow XCom."""

    bucket: str
    key: str
    dataset: str
    row_count: int
    run_date: str
    run_id: str


def _sanitise_run_id(run_id: str) -> str:
    """Return a safe deterministic S3 key segment for an Airflow run ID."""
    sanitised_run_id = re.sub(r"[^A-Za-z0-9_.-]+", "-", run_id).strip(".-")
    if not sanitised_run_id:
        raise ValueError("Airflow run_id must contain at least one safe key character.")
    return sanitised_run_id


def _task_run_identity() -> tuple[str, str]:
    """Read logical run identity in the Airflow wrapper, never in pure ETL modules."""
    context = get_current_context()
    logical_date = context["logical_date"]
    if isinstance(logical_date, datetime):
        run_date = logical_date.date().isoformat()
    elif isinstance(logical_date, date):
        run_date = logical_date.isoformat()
    else:
        raise TypeError("Airflow logical_date must be date-compatible.")
    return run_date, str(context["run_id"])


def _work_reference(dataset: str, stage: str, run_date: str, run_id: str) -> S3DatasetReference:
    """Build one configured intermediate S3 reference without carrying data in XCom."""
    config: dict[str, Any] = load_config()
    try:
        bucket = config["s3"]["bucket_name"]
        work_prefix = config["s3"]["work_zone"]["prefix"]
    except KeyError as error:
        raise ValueError(f"Missing work-zone S3 configuration: {error}") from error
    safe_run_id = _sanitise_run_id(run_id)
    key = f"{work_prefix}/{stage}/{dataset}/run_date={run_date}/run_id={safe_run_id}/{dataset}.parquet"
    return {
        "bucket": bucket,
        "key": key,
        "dataset": dataset,
        "row_count": 0,
        "run_date": run_date,
        "run_id": run_id,
    }


def _write_intermediate_dataframe(
    dataframe: pd.DataFrame, dataset: str, stage: str, run_date: str, run_id: str
) -> S3DatasetReference:
    """Persist an in-memory task result to configured work S3 and return metadata."""
    reference = _work_reference(dataset, stage, run_date, run_id)
    config = load_config()
    aws_conn_id = config["aws"]["connection_id"]
    s3_uri = f"s3://{reference['bucket']}/{reference['key']}"
    _, storage_options = get_storage_options(aws_conn_id)
    dataframe.to_parquet(s3_uri, engine="pyarrow", compression="snappy", index=False, storage_options=storage_options)
    reference["row_count"] = len(dataframe)
    logger.info("Work dataset persisted: dataset=%s rows=%s key=%s", dataset, len(dataframe), reference["key"])
    return reference


def _read_intermediate_dataframe(reference: S3DatasetReference) -> pd.DataFrame:
    """Load an intermediate task dataset from its S3 reference."""
    config = load_config()
    aws_conn_id = config["aws"]["connection_id"]
    s3_uri = f"s3://{reference['bucket']}/{reference['key']}"
    _, storage_options = get_storage_options(aws_conn_id)
    dataframe = pd.read_parquet(s3_uri, storage_options=storage_options)
    logger.info("Work dataset loaded: dataset=%s rows=%s key=%s", reference["dataset"], len(dataframe), reference["key"])
    return dataframe


@task(task_id="extract_sales", retries=2, retry_delay=RETRY_DELAY)
def extract_sales_task() -> S3DatasetReference:
    """Extract RAW sales and persist an intermediate S3 representation for later tasks."""
    run_date, run_id = _task_run_identity()
    return _write_intermediate_dataframe(extract_sales(), "sales", "extracted", run_date, run_id)


@task(task_id="extract_products", retries=2, retry_delay=RETRY_DELAY)
def extract_products_task() -> S3DatasetReference:
    """Extract RAW products and persist an intermediate S3 representation for later tasks."""
    run_date, run_id = _task_run_identity()
    return _write_intermediate_dataframe(extract_products(), "products", "extracted", run_date, run_id)


def _input_validation_metadata(reference: S3DatasetReference, is_valid: bool, issue_count: int) -> dict[str, Any]:
    """Attach small non-blocking validation metadata to an S3 dataset reference."""
    return {
        **reference,
        "input_validation": {"is_valid": is_valid, "issue_count": issue_count},
    }


@task(task_id="validate_sales_input")
def validate_sales_input_task(reference: S3DatasetReference) -> dict[str, Any]:
    """Run non-blocking sales input validation and return only reference metadata."""
    result = validate_sales_input(_read_intermediate_dataframe(reference))
    logger.info("Sales input validation completed: valid=%s issues=%s", result.is_valid, result.error_count)
    return _input_validation_metadata(reference, result.is_valid, result.error_count)


@task(task_id="validate_products_input")
def validate_products_input_task(reference: S3DatasetReference) -> dict[str, Any]:
    """Run non-blocking products input validation and return only reference metadata."""
    result = validate_products_input(_read_intermediate_dataframe(reference))
    logger.info("Products input validation completed: valid=%s issues=%s", result.is_valid, result.error_count)
    return _input_validation_metadata(reference, result.is_valid, result.error_count)


@task(task_id="transform_retail")
def transform_retail_task(
    sales_reference: S3DatasetReference, products_reference: S3DatasetReference
) -> dict[str, Any]:
    """Transform/join datasets, persist rejected rows, and return transformed S3 metadata."""
    sales_dataframe = _read_intermediate_dataframe(sales_reference)
    products_dataframe = _read_intermediate_dataframe(products_reference)
    result = transform_retail(sales_dataframe, products_dataframe)
    run_date, run_id = sales_reference["run_date"], sales_reference["run_id"]
    sales_rejected = write_rejected_records(result.sales_rejected_rows, "sales", run_date, run_id)
    products_rejected = write_rejected_records(result.products_rejected_rows, "products", run_date, run_id)
    transformed_reference = _write_intermediate_dataframe(
        result.transformed_rows, "retail", "transformed", run_date, run_id
    )
    logger.info(
        "Retail transformation task completed: transformed=%s sales_rejected=%s products_rejected=%s",
        transformed_reference["row_count"], sales_rejected.row_count, products_rejected.row_count,
    )
    return {
        "transformed": transformed_reference,
        "sales_rejected": asdict(sales_rejected),
        "products_rejected": asdict(products_rejected),
    }


@task(task_id="validate_retail_output", retries=0)
def validate_retail_output_task(reference: S3DatasetReference) -> dict[str, Any]:
    """Apply the blocking output gate; an exception stops all downstream tasks."""
    validated_dataframe = validate_retail_output(_read_intermediate_dataframe(reference))
    logger.info("Strict output validation task completed: rows=%s", len(validated_dataframe))
    return {**reference, "output_validation": {"is_valid": True}}


@task(task_id="write_processed_data", retries=2, retry_delay=RETRY_DELAY)
def write_processed_data_task(reference: S3DatasetReference) -> dict[str, Any]:
    """Persist already-approved analytical data to the processed S3 zone."""
    write_result = write_processed_data(
        _read_intermediate_dataframe(reference),
        run_date=reference["run_date"],
        run_id=reference["run_id"],
    )
    return asdict(write_result)
