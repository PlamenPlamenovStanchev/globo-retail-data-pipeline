"""Non-blocking, observational validation for RAW input datasets."""

from dataclasses import dataclass

import pandas as pd
import pandera.pandas as pa

from include.utils.logger import setup_logger
from include.validations.schemas.products_input_schema import PRODUCTS_INPUT_SCHEMA
from include.validations.schemas.sales_input_schema import SALES_INPUT_SCHEMA


logger = setup_logger(__name__)
MAX_REPORTED_ISSUES = 5


@dataclass
class InputValidationResult:
    """The original input data and its non-blocking schema-validation status."""

    dataframe: pd.DataFrame
    is_valid: bool
    error_count: int
    validation_errors: list[str]


def _summarize_failure_cases(failure_cases: pd.DataFrame) -> list[str]:
    """Create a small, readable summary from Pandera lazy-validation failures."""
    if failure_cases.empty:
        return ["Schema validation failed without detailed failure cases."]

    summaries: list[str] = []
    seen: set[tuple[str, str]] = set()
    for _, failure in failure_cases.iterrows():
        column_value = failure.get("column")
        if pd.isna(column_value):
            column_value = failure.get("failure_case")
        column = str(column_value or "dataframe")
        check = str(failure.get("check") or failure.get("schema_context") or "schema check")
        issue = (column, check)
        if issue not in seen:
            seen.add(issue)
            summaries.append(f"Column '{column}' failed {check}")
        if len(summaries) == MAX_REPORTED_ISSUES:
            break

    return summaries


def _validate_input(
    dataframe: pd.DataFrame,
    schema: pa.DataFrameSchema,
    dataset_name: str,
) -> InputValidationResult:
    """Observe schema failures without altering data or blocking downstream work."""
    try:
        schema.validate(dataframe, lazy=True)
    except pa.errors.SchemaErrors as error:
        failure_cases: pd.DataFrame = error.failure_cases
        validation_errors = _summarize_failure_cases(failure_cases)
        error_count = len(failure_cases)
        logger.warning(
            "%s input validation detected %s schema issues; pipeline will continue",
            dataset_name.capitalize(),
            error_count,
        )
        logger.warning("%s input validation issues: %s", dataset_name.capitalize(), "; ".join(validation_errors))
        return InputValidationResult(dataframe, False, error_count, validation_errors)
    except pa.errors.SchemaError as error:
        validation_errors = [str(error)]
        logger.warning(
            "%s input validation detected 1 schema issue; pipeline will continue",
            dataset_name.capitalize(),
        )
        logger.warning("%s input validation issues: %s", dataset_name.capitalize(), validation_errors[0])
        return InputValidationResult(dataframe, False, 1, validation_errors)

    logger.info("%s input validation passed: rows=%s", dataset_name.capitalize(), len(dataframe))
    return InputValidationResult(dataframe, True, 0, [])


def validate_sales_input(dataframe: pd.DataFrame) -> InputValidationResult:
    """Validate RAW sales structure and return the unchanged input with diagnostics."""
    return _validate_input(dataframe, SALES_INPUT_SCHEMA, "sales")


def validate_products_input(dataframe: pd.DataFrame) -> InputValidationResult:
    """Validate RAW products structure and return the unchanged input with diagnostics."""
    return _validate_input(dataframe, PRODUCTS_INPUT_SCHEMA, "products")
