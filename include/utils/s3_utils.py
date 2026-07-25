from typing import Any

from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def get_storage_options(aws_conn_id: str) -> dict[str, Any]:
    """
    Get storage options for S3 connection.

    :param aws_conn_id: The Airflow connection ID for AWS.
    :return: A dictionary containing storage options.
    """
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    credentials = s3_hook.get_credentials()

    # pandas forwards these options to s3fs when it opens an s3:// URI.
    storage_options = {
        "key": credentials.access_key,
        "secret": credentials.secret_key,
        "token": credentials.token,
    }

    return storage_options
