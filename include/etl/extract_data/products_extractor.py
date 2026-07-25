import pandas as pd

from include.utils.config_loader import load_config
from include.utils.logger import setup_logger
from include.utils.s3_utils import get_storage_options


logger = setup_logger(__name__)


def extract_products() -> pd.DataFrame:
    """Read the configured product JSON from S3 and return it as a DataFrame."""
    config = load_config()

    try:
        bucket_name = config["s3"]["bucket_name"]
        products_key = config["s3"]["raw_zone"]["products_key"]
        aws_conn_id = config["aws"]["connection_id"]
    except KeyError as error:
        raise ValueError(f"Missing required product S3 configuration: {error}") from error

    s3_uri = f"s3://{bucket_name}/{products_key}"

    try:
        # Airflow resolves the connection; pandas reads the S3 object directly.
        storage_options = get_storage_options(aws_conn_id)
        logger.info("Extracting product data from bucket=%s, key=%s", bucket_name, products_key)
        products_data = pd.read_json(s3_uri, storage_options=storage_options)
    except Exception as error:
        logger.exception("Product extraction failed for bucket=%s, key=%s", bucket_name, products_key)
        raise RuntimeError(
            f"Unable to extract product data from S3 bucket '{bucket_name}' with key '{products_key}'."
        ) from error

    logger.info("Products extracted successfully: %s rows", len(products_data))
    return products_data
