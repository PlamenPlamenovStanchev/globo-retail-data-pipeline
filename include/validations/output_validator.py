"""Blocking validation for the transformed retail dataset."""

import pandas as pd
import pandera.pandas as pa

from include.utils.logger import setup_logger
from include.validations.schemas.retail_output_schema import RETAIL_OUTPUT_SCHEMA


logger = setup_logger(__name__)


def validate_retail_output(data: pd.DataFrame) -> pd.DataFrame:
    """Validate transformed retail data and raise an error when validation fails."""
    try:
        validated_data: pd.DataFrame = RETAIL_OUTPUT_SCHEMA.validate(data, lazy=True)
    except (pa.errors.SchemaError, pa.errors.SchemaErrors):
        # Re-raise so the Airflow task fails instead of loading bad transformed data.
        logger.exception("Transformed retail data failed validation; stopping the pipeline")
        raise

    logger.info("Output validation passed for transformed retail data (%s rows)", len(validated_data))
    return validated_data
