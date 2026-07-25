"""Pure transformation of RAW product records into analytical product data."""

import pandas as pd

from include.etl.transform_data.transformation_result import (
    TransformationResult,
    add_rejection_reason,
    build_rejected_rows,
    require_columns,
)
from include.utils.logger import setup_logger


logger = setup_logger(__name__)
PRODUCT_COLUMNS = ("product_id", "category", "brand", "rating", "in_stock", "launch_date")
BOOLEAN_VALUES = {"true": True, "false": False, "1": True, "0": False}


def _parse_boolean(values: pd.Series) -> pd.Series:
    """Parse booleans and recognized textual/0-1 representations only."""
    def parse_value(value: object) -> object:
        if pd.isna(value):
            return pd.NA
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return BOOLEAN_VALUES.get(value.strip().lower(), pd.NA)
        if isinstance(value, (int, float)) and value in (0, 1):
            return bool(value)
        return pd.NA

    return values.map(parse_value).astype("boolean")


def transform_products(dataframe: pd.DataFrame) -> TransformationResult:
    """Standardize RAW products and separate rows with conversion failures."""
    require_columns(dataframe, PRODUCT_COLUMNS, "products")
    source_data = dataframe.copy(deep=True)
    transformed_data = source_data.copy(deep=True)
    rejection_reasons = pd.Series("", index=transformed_data.index, dtype="string")

    product_ids = pd.to_numeric(transformed_data["product_id"], errors="coerce")
    invalid_product_ids = product_ids.isna() | product_ids.mod(1).ne(0)
    add_rejection_reason(rejection_reasons, invalid_product_ids, "product_id: invalid integer")
    transformed_data["product_id"] = product_ids

    ratings = pd.to_numeric(transformed_data["rating"], errors="coerce")
    add_rejection_reason(rejection_reasons, ratings.isna(), "rating: invalid numeric value")
    transformed_data["rating"] = ratings

    parsed_booleans = _parse_boolean(transformed_data["in_stock"])
    add_rejection_reason(rejection_reasons, parsed_booleans.isna(), "in_stock: invalid boolean")
    transformed_data["in_stock"] = parsed_booleans

    parsed_launch_dates = pd.to_datetime(transformed_data["launch_date"], format="%Y-%m-%d", errors="coerce")
    invalid_launch_dates = transformed_data["launch_date"].notna() & parsed_launch_dates.isna()
    add_rejection_reason(rejection_reasons, invalid_launch_dates, "launch_date: invalid date")
    transformed_data["launch_date"] = parsed_launch_dates

    transformed_data["category"] = transformed_data["category"].astype("string").str.strip()
    transformed_data["brand"] = transformed_data["brand"].astype("string").str.strip()

    # Business-invalid rows are quarantined rather than adjusted to pass the
    # final analytical schema.
    add_rejection_reason(rejection_reasons, transformed_data["product_id"].le(0), "product_id: must be positive")
    add_rejection_reason(rejection_reasons, transformed_data["product_id"].duplicated(keep=False), "product_id: duplicate")
    add_rejection_reason(
        rejection_reasons,
        transformed_data["rating"].lt(1) | transformed_data["rating"].gt(5),
        "rating: must be between one and five",
    )
    add_rejection_reason(
        rejection_reasons,
        transformed_data["category"].isna() | transformed_data["category"].eq(""),
        "category: missing or empty",
    )
    add_rejection_reason(
        rejection_reasons,
        transformed_data["brand"].isna() | transformed_data["brand"].eq(""),
        "brand: missing or empty",
    )

    rejected_rows = build_rejected_rows(source_data, rejection_reasons)
    clean_rows = transformed_data.loc[rejection_reasons.eq("")].copy()
    clean_rows["product_id"] = clean_rows["product_id"].astype("int64")
    clean_rows["rating"] = clean_rows["rating"].astype("float64")
    # All nullable/invalid values were rejected above; use the plain dtype the
    # strict analytical schema expects after the Parquet round-trip.
    clean_rows["in_stock"] = clean_rows["in_stock"].astype(bool)
    clean_rows = clean_rows[list(PRODUCT_COLUMNS)]

    logger.info(
        "Products transformation completed: input=%s transformed=%s rejected=%s",
        len(source_data),
        len(clean_rows),
        len(rejected_rows),
    )
    return TransformationResult(clean_rows, rejected_rows)
