from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def get_storage_options(aws_conn_id: str) -> dict:
    """
    Get storage options for S3 connection.

    :param aws_conn_id: The Airflow connection ID for AWS.
    :return: A dictionary containing storage options.
    """
    s3_hook = S3Hook(connection_id="my_aws_conn")
    credentials = s3_hook.get_credentials()

    storage_options = {
        "key": credentials.access_key,
        "secret": credentials.secret_key,
    }

    return s3_hook, storage_options