"""Hard quality gate for the final transformed retail dataset."""

import pandas as pd
import pandera.pandas as pa

from include.exceptions.pipeline_exceptions import OutputValidationError
from include.utils.logger import setup_logger
from include.validations.schemas.retail_output_schema import RETAIL_OUTPUT_SCHEMA


logger = setup_logger(__name__)
MAX_REPORTED_ISSUES = 5


def _summarize_failure_cases(failure_cases: pd.DataFrame) -> tuple[list[str], list[str]]:
    """Return bounded readable issues and affected columns from Pandera failures."""
    if failure_cases.empty:
        return ["Schema validation failed without detailed failure cases."], []

    summaries: list[str] = []
    affected_columns: list[str] = []
    seen: set[tuple[str, str]] = set()
    for _, failure in failure_cases.iterrows():
        column_value = failure.get("column")
        if pd.isna(column_value):
            column_value = failure.get("failure_case")
        column = str(column_value or "dataframe")
        check = str(failure.get("check") or failure.get("schema_context") or "schema check")
        if column not in affected_columns:
            affected_columns.append(column)
        issue = (column, check)
        if issue not in seen and len(summaries) < MAX_REPORTED_ISSUES:
            seen.add(issue)
            summaries.append(f"Column '{column}' failed {check}")

    return summaries, affected_columns


def validate_retail_output(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Approve valid transformed data for processed S3 or raise a hard quality-gate error.

    A successful return means the DataFrame is approved for processed-zone persistence.
    An ``OutputValidationError`` means it must not be written to processed S3 or sent
    to downstream Snowflake ELT.
    """
    if not isinstance(dataframe, pd.DataFrame):
        raise TypeError("Output validation requires a pandas DataFrame.")
    if dataframe.empty:
        logger.error("Output validation failed: transformed dataset is empty")
        raise OutputValidationError("Output validation failed: transformed dataset is empty.")

    try:
        validated_dataframe: pd.DataFrame = RETAIL_OUTPUT_SCHEMA.validate(dataframe, lazy=True)
    except pa.errors.SchemaErrors as error:
        failure_cases: pd.DataFrame = error.failure_cases
        summaries, affected_columns = _summarize_failure_cases(failure_cases)
        failure_count = len(failure_cases)
        logger.error(
            "Strict output validation failed: failures=%s affected_columns=%s",
            failure_count,
            affected_columns,
        )
        logger.error("Strict output validation sample issues: %s", "; ".join(summaries))
        raise OutputValidationError(
            f"Output validation failed with {failure_count} data-quality violations. "
            f"Affected columns: {affected_columns}."
        ) from error
    except pa.errors.SchemaError as error:
        logger.error("Strict output validation failed: failures=1 affected_columns=['dataframe']")
        raise OutputValidationError("Output validation failed with 1 data-quality violation.") from error

    logger.info("Strict output validation passed: rows=%s", len(validated_dataframe))
    return validated_dataframe
