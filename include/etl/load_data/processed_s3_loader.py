"""Direct persistence of validated analytical data in the S3 processed zone."""

from dataclasses import dataclass
from datetime import date
from typing import Any

from botocore.exceptions import BotoCoreError, ClientError
import pandas as pd
from pyarrow import ArrowException

from include.utils.config_loader import load_config
from include.utils.logger import setup_logger
from include.utils.run_utils import normalise_run_date, sanitise_run_id
from include.utils.s3_utils import get_storage_options


logger = setup_logger(__name__)


@dataclass(frozen=True)
class ProcessedWriteResult:
    """Safe metadata describing a successful processed-zone Parquet write."""

    bucket: str
    key: str
    row_count: int
    format: str
    s3_uri: str


def write_processed_data(
    dataframe: pd.DataFrame,
    run_date: date | str,
    run_id: str | None = None,
) -> ProcessedWriteResult:
    """Write already-validated analytical data directly to processed-zone Parquet.

    The caller is responsible for strict output validation before calling this
    function. This loader writes the supplied DataFrame unchanged.
    """
    if not isinstance(dataframe, pd.DataFrame):
        raise TypeError("dataframe must be a pandas DataFrame.")
    if dataframe.empty:
        raise ValueError("Cannot write an empty processed dataset.")

    config: dict[str, Any] = load_config()
    try:
        bucket = config["s3"]["bucket_name"]
        processed_prefix = config["s3"]["processed_zone"]["sales_clean_prefix"]
        aws_conn_id = config["aws"]["connection_id"]
    except KeyError as error:
        raise ValueError(f"Missing processed S3 configuration: {error}") from error

    partition_date = normalise_run_date(run_date)
    key_parts = [processed_prefix, f"run_date={partition_date}"]
    if run_id is not None:
        key_parts.append(f"run_id={sanitise_run_id(run_id)}")
    key = "/".join(key_parts + ["sales_clean.parquet"])
    s3_uri = f"s3://{bucket}/{key}"

    try:
        storage_options = get_storage_options(aws_conn_id)
        logger.info("Writing validated processed dataset to S3: rows=%s key=%s", len(dataframe), key)
        # s3fs writes the in-memory DataFrame directly; no local staging file is used.
        dataframe.to_parquet(
            s3_uri,
            engine="pyarrow",
            compression="snappy",
            index=False,
            storage_options=storage_options,
        )
    except (ArrowException, BotoCoreError, ClientError, ImportError, OSError, ValueError):
        logger.exception("Failed to write processed dataset to S3: key=%s", key)
        raise

    logger.info("Processed dataset written successfully: rows=%s key=%s", len(dataframe), key)
    return ProcessedWriteResult(
        bucket=bucket,
        key=key,
        row_count=len(dataframe),
        format="parquet",
        s3_uri=s3_uri,
    )
