"""Persistence of transformation-rejected records in the S3 rejected zone."""

from dataclasses import dataclass
from typing import Any

from botocore.exceptions import BotoCoreError, ClientError
import pandas as pd
from pyarrow import ArrowException

from include.utils.config_loader import load_config
from include.utils.logger import setup_logger
from include.utils.s3_utils import get_storage_options


logger = setup_logger(__name__)
SUPPORTED_DATASETS = frozenset({"sales", "products"})


@dataclass(frozen=True)
class S3WriteResult:
    """Metadata describing a rejected-record S3 write attempt."""

    bucket: str
    key: str | None
    row_count: int
    written: bool


def write_rejected_records(
    dataframe: pd.DataFrame,
    dataset_name: str,
) -> S3WriteResult:
    """Write an already-prepared rejected DataFrame to its configured S3 key."""
    if not isinstance(dataframe, pd.DataFrame):
        raise TypeError("dataframe must be a pandas DataFrame.")
    if dataset_name not in SUPPORTED_DATASETS:
        raise ValueError(f"Unsupported rejected dataset: {dataset_name!r}.")

    config: dict[str, Any] = load_config()
    try:
        bucket = config["s3"]["bucket_name"]
        key = config["s3"]["rejected_zone"][f"rejected_{dataset_name}_key"]
        aws_conn_id = config["aws"]["connection_id"]
    except KeyError as error:
        raise ValueError(f"Missing rejected-record S3 configuration: {error}") from error

    if dataframe.empty:
        logger.info("No rejected %s records to persist", dataset_name)
        return S3WriteResult(bucket=bucket, key=None, row_count=0, written=False)

    s3_uri = f"s3://{bucket}/{key}"

    try:
        storage_options = get_storage_options(aws_conn_id)
        # Write directly through s3fs; do not stage the DataFrame on local disk.
        dataframe.to_parquet(s3_uri, engine="pyarrow", index=False, storage_options=storage_options)
    except (ArrowException, BotoCoreError, ClientError, ImportError, OSError, ValueError):
        logger.exception("Failed to write rejected %s records to S3: key=%s", dataset_name, key)
        raise

    logger.info("Rejected %s records written to S3: rows=%s key=%s", dataset_name, len(dataframe), key)
    return S3WriteResult(bucket=bucket, key=key, row_count=len(dataframe), written=True)
