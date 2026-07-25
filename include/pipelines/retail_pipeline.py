"""Thin Airflow TaskFlow wrappers for the retail ETL pipeline."""

from dataclasses import asdict
from datetime import date, datetime, timedelta
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
from include.utils.run_utils import normalise_run_date, sanitise_run_id
from include.utils.s3_utils import get_storage_options
from include.validations.input_validator import validate_products_input, validate_sales_input
from include.validations.output_validator import validate_retail_output


logger = setup_logger(__name__)
RETRY_DELAY = timedelta(seconds=30)


class S3DatasetReference(TypedDict):
    """Small JSON-serializable metadata exchanged through Airflow XCom."""

    bucket: str
    key: str
    dataset: str
    row_count: int
    run_date: str
    run_id: str


def _task_run_identity() -> tuple[str, str]:
    """Return the logical Airflow run date and identifier."""
    context = get_current_context()
    logical_date = context["logical_date"]
    if not isinstance(logical_date, (datetime, date)):
        raise TypeError("Airflow logical_date must be date-compatible.")
    return normalise_run_date(logical_date), str(context["run_id"])


def _work_reference(dataset: str, stage: str, run_date: str, run_id: str) -> S3DatasetReference:
    """Build a configured work-zone object reference without carrying data in XCom."""
    config: dict[str, Any] = load_config()
    try:
        bucket = config["s3"]["bucket_name"]
        work_prefix = config["s3"]["work_zone"]["prefix"]
    except KeyError as error:
        raise ValueError(f"Missing work-zone S3 configuration: {error}") from error

    key = (
        f"{work_prefix}/{stage}/{dataset}/run_date={run_date}/"
        f"run_id={sanitise_run_id(run_id)}/{dataset}.parquet"
    )
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
    """Write one task boundary dataset to the configured work zone."""
    reference = _work_reference(dataset, stage, run_date, run_id)
    aws_conn_id = load_config()["aws"]["connection_id"]
    dataframe.to_parquet(
        f"s3://{reference['bucket']}/{reference['key']}",
        engine="pyarrow",
        compression="snappy",
        index=False,
        storage_options=get_storage_options(aws_conn_id),
    )
    reference["row_count"] = len(dataframe)
    logger.info("Work dataset persisted: dataset=%s rows=%s key=%s", dataset, len(dataframe), reference["key"])
    return reference


def _read_intermediate_dataframe(reference: S3DatasetReference) -> pd.DataFrame:
    """Read a work-zone dataset referenced by lightweight task metadata."""
    aws_conn_id = load_config()["aws"]["connection_id"]
    dataframe = pd.read_parquet(
        f"s3://{reference['bucket']}/{reference['key']}",
        storage_options=get_storage_options(aws_conn_id),
    )
    logger.info("Work dataset loaded: dataset=%s rows=%s key=%s", reference["dataset"], len(dataframe), reference["key"])
    return dataframe


def _validated_reference(reference: S3DatasetReference, is_valid: bool, issue_count: int) -> dict[str, Any]:
    """Attach non-blocking input-validation metadata to a reference."""
    return {**reference, "input_validation": {"is_valid": is_valid, "issue_count": issue_count}}


@task(task_id="extract_validate_sales", retries=2, retry_delay=RETRY_DELAY)
def extract_validate_sales_task() -> dict[str, Any]:
    """Extract sales, log non-blocking RAW validation, and persist one work object."""
    result = validate_sales_input(extract_sales())
    logger.info("Sales input validation completed: valid=%s issues=%s", result.is_valid, result.error_count)
    run_date, run_id = _task_run_identity()
    reference = _write_intermediate_dataframe(result.dataframe, "sales", "extracted", run_date, run_id)
    return _validated_reference(reference, result.is_valid, result.error_count)


@task(task_id="extract_validate_products", retries=2, retry_delay=RETRY_DELAY)
def extract_validate_products_task() -> dict[str, Any]:
    """Extract products, log non-blocking RAW validation, and persist one work object."""
    result = validate_products_input(extract_products())
    logger.info("Products input validation completed: valid=%s issues=%s", result.is_valid, result.error_count)
    run_date, run_id = _task_run_identity()
    reference = _write_intermediate_dataframe(result.dataframe, "products", "extracted", run_date, run_id)
    return _validated_reference(reference, result.is_valid, result.error_count)


@task(task_id="transform_retail")
def transform_retail_task(sales_reference: S3DatasetReference, products_reference: S3DatasetReference) -> dict[str, Any]:
    """Transform source references and persist rejected records to the rejected zone."""
    result = transform_retail(
        _read_intermediate_dataframe(sales_reference), _read_intermediate_dataframe(products_reference)
    )
    run_date, run_id = sales_reference["run_date"], sales_reference["run_id"]
    sales_rejected = write_rejected_records(result.sales_rejected_rows, "sales", run_date, run_id)
    products_rejected = write_rejected_records(result.products_rejected_rows, "products", run_date, run_id)
    transformed_reference = _write_intermediate_dataframe(result.transformed_rows, "retail", "transformed", run_date, run_id)
    logger.info(
        "Retail transformation completed: transformed=%s sales_rejected=%s products_rejected=%s",
        transformed_reference["row_count"], sales_rejected.row_count, products_rejected.row_count,
    )
    return {
        "transformed": transformed_reference,
        "sales_rejected": asdict(sales_rejected),
        "products_rejected": asdict(products_rejected),
    }


@task(task_id="validate_output", retries=0)
def validate_output_task(reference: S3DatasetReference) -> dict[str, Any]:
    """Strictly validate transformed output; failures stop downstream loading."""
    validated_dataframe = validate_retail_output(_read_intermediate_dataframe(reference))
    logger.info("Strict output validation completed: rows=%s", len(validated_dataframe))
    return {**reference, "output_validation": {"is_valid": True}}


@task(task_id="load_processed", retries=2, retry_delay=RETRY_DELAY)
def load_processed_task(reference: S3DatasetReference) -> dict[str, Any]:
    """Write output approved by the strict validation task to the processed zone."""
    return asdict(
        write_processed_data(
            _read_intermediate_dataframe(reference), run_date=reference["run_date"], run_id=reference["run_id"]
        )
    )
