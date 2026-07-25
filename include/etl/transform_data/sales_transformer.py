"""Pure transformation of RAW sales records into analytical sales data."""

import pandas as pd

from include.etl.transform_data.transformation_result import (
    TransformationResult,
    add_rejection_reason,
    build_rejected_rows,
    require_columns,
)
from include.utils.logger import setup_logger


logger = setup_logger(__name__)
ALLOWED_REGIONS = frozenset({"North", "South", "East", "West"})
ALLOWED_ORDER_STATUSES = frozenset({"Completed", "Shipped", "Pending", "Returned"})
SALES_COLUMN_MAPPING = {
    "sales id": "sales_id",
    "proDuct Id": "product_id",
    "Region": "region",
    "qty": "quantity",
    "Price": "price",
    "Time stamp": "timestamp",
    "discount": "discount",
    "order_status": "order_status",
}


def _invalid_integer_values(values: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Return numeric values and a mask for values not safely representable as integers."""
    numeric_values = pd.to_numeric(values, errors="coerce")
    invalid_values = numeric_values.isna() | numeric_values.mod(1).ne(0)
    return numeric_values, invalid_values


def transform_sales(dataframe: pd.DataFrame) -> TransformationResult:
    """Standardize RAW sales records and separate rows with conversion failures."""
    require_columns(dataframe, tuple(SALES_COLUMN_MAPPING), "sales")
    source_data = dataframe.copy(deep=True)
    transformed_data = source_data.rename(columns=SALES_COLUMN_MAPPING).copy()
    rejection_reasons = pd.Series("", index=transformed_data.index, dtype="string")

    for column in ("sales_id", "product_id", "quantity"):
        numeric_values, invalid_values = _invalid_integer_values(transformed_data[column])
        add_rejection_reason(rejection_reasons, invalid_values, f"{column}: invalid integer")
        transformed_data[column] = numeric_values

    for column in ("price", "discount"):
        numeric_values = pd.to_numeric(transformed_data[column], errors="coerce")
        add_rejection_reason(rejection_reasons, numeric_values.isna(), f"{column}: invalid numeric value")
        transformed_data[column] = numeric_values

    parsed_timestamps = pd.to_datetime(
        transformed_data["timestamp"], format="%d-%m-%y %H:%M", errors="coerce"
    )
    add_rejection_reason(rejection_reasons, parsed_timestamps.isna(), "timestamp: invalid datetime")
    transformed_data["timestamp"] = parsed_timestamps

    transformed_data["region"] = transformed_data["region"].astype("string").str.strip().str.title()
    transformed_data["order_status"] = (
        transformed_data["order_status"].astype("string").str.strip().str.title()
    )

    # Keep the strict output contract reachable by rejecting business-invalid
    # source rows instead of silently changing their values.
    add_rejection_reason(rejection_reasons, transformed_data["sales_id"].le(0), "sales_id: must be positive")
    add_rejection_reason(rejection_reasons, transformed_data["product_id"].le(0), "product_id: must be positive")
    add_rejection_reason(rejection_reasons, transformed_data["quantity"].le(0), "quantity: must be greater than zero")
    add_rejection_reason(rejection_reasons, transformed_data["price"].lt(0), "price: must be non-negative")
    add_rejection_reason(
        rejection_reasons,
        transformed_data["discount"].lt(0) | transformed_data["discount"].gt(1),
        "discount: must be between zero and one",
    )
    add_rejection_reason(rejection_reasons, transformed_data["region"].isna(), "region: missing")
    add_rejection_reason(
        rejection_reasons, ~transformed_data["region"].isin(ALLOWED_REGIONS), "region: invalid value"
    )
    add_rejection_reason(
        rejection_reasons,
        ~transformed_data["order_status"].isin(ALLOWED_ORDER_STATUSES),
        "order_status: invalid value",
    )
    add_rejection_reason(rejection_reasons, transformed_data["sales_id"].duplicated(keep=False), "sales_id: duplicate")

    rejected_rows = build_rejected_rows(source_data, rejection_reasons)
    clean_rows = transformed_data.loc[rejection_reasons.eq("")].copy()
    clean_rows["sales_id"] = clean_rows["sales_id"].astype("int64")
    clean_rows["product_id"] = clean_rows["product_id"].astype("int64")
    clean_rows["quantity"] = clean_rows["quantity"].astype("int64")
    clean_rows["price"] = clean_rows["price"].astype("float64")
    clean_rows["discount"] = clean_rows["discount"].astype("float64")
    clean_rows["gross_revenue"] = clean_rows["quantity"] * clean_rows["price"]
    clean_rows["discount_amount"] = clean_rows["gross_revenue"] * clean_rows["discount"]
    clean_rows["net_revenue"] = clean_rows["gross_revenue"] - clean_rows["discount_amount"]
    clean_rows = clean_rows[
        [
            "sales_id", "product_id", "timestamp", "region", "quantity", "price", "discount",
            "order_status", "gross_revenue", "discount_amount", "net_revenue",
        ]
    ]

    logger.info(
        "Sales transformation completed: input=%s transformed=%s rejected=%s",
        len(source_data),
        len(clean_rows),
        len(rejected_rows),
    )
    return TransformationResult(clean_rows, rejected_rows)
