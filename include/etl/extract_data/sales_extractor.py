import pandas as pd

from include.utils.config_loader import load_config
from include.utils.logger import setup_logger
from include.utils.s3_utils import get_storage_options


logger = setup_logger(__name__)


def extract_sales() -> pd.DataFrame:
    """Read the configured sales CSV from S3 and return it as a DataFrame."""
    config = load_config()

    try:
        bucket_name = config["s3"]["bucket_name"]
        sales_key = config["s3"]["raw_zone"]["sales_key"]
        aws_conn_id = config["aws"]["connection_id"]
    except KeyError as error:
        raise ValueError(f"Missing required sales S3 configuration: {error}") from error

    s3_uri = f"s3://{bucket_name}/{sales_key}"

    try:
        # Airflow resolves the connection; pandas reads the S3 object directly.
        storage_options = get_storage_options(aws_conn_id)
        logger.info("Extracting sales data from bucket=%s, key=%s", bucket_name, sales_key)
        sales_data = pd.read_csv(s3_uri, storage_options=storage_options)
    except Exception as error:
        logger.exception("Sales extraction failed for bucket=%s, key=%s", bucket_name, sales_key)
        raise RuntimeError(
            f"Unable to extract sales data from S3 bucket '{bucket_name}' with key '{sales_key}'."
        ) from error

    logger.info("Sales extracted successfully: %s rows", len(sales_data))
    return sales_data
