"""Strict Pandera contract for transformed analytical retail data."""

import numpy as np
import pandas as pd
import pandera.pandas as pa


ALLOWED_REGIONS = ("North", "South", "East", "West")
ALLOWED_ORDER_STATUSES = ("Completed", "Shipped", "Pending", "Returned")
FLOAT_RTOL = 1e-9
FLOAT_ATOL = 1e-8


def _is_not_empty(dataframe: pd.DataFrame) -> bool:
    """Require at least one transformed analytical row."""
    return not dataframe.empty


def _gross_revenue_is_consistent(dataframe: pd.DataFrame) -> bool:
    """Check gross revenue using a small explicit floating-point tolerance."""
    try:
        expected = dataframe["quantity"] * dataframe["price"]
        return bool(np.isclose(dataframe["gross_revenue"], expected, rtol=FLOAT_RTOL, atol=FLOAT_ATOL).all())
    except (TypeError, ValueError):
        return False


def _discount_amount_is_consistent(dataframe: pd.DataFrame) -> bool:
    """Check discount amount using a small explicit floating-point tolerance."""
    try:
        expected = dataframe["gross_revenue"] * dataframe["discount"]
        return bool(np.isclose(dataframe["discount_amount"], expected, rtol=FLOAT_RTOL, atol=FLOAT_ATOL).all())
    except (TypeError, ValueError):
        return False


def _net_revenue_is_consistent(dataframe: pd.DataFrame) -> bool:
    """Check net revenue using a small explicit floating-point tolerance."""
    try:
        expected = dataframe["gross_revenue"] - dataframe["discount_amount"]
        return bool(np.isclose(dataframe["net_revenue"], expected, rtol=FLOAT_RTOL, atol=FLOAT_ATOL).all())
    except (TypeError, ValueError):
        return False


RETAIL_OUTPUT_SCHEMA: pa.DataFrameSchema = pa.DataFrameSchema(
    {
        "sales_id": pa.Column(int, checks=pa.Check.gt(0), nullable=False, unique=True),
        "product_id": pa.Column(int, checks=pa.Check.gt(0), nullable=False),
        "timestamp": pa.Column(pa.DateTime, nullable=False),
        "region": pa.Column(str, checks=pa.Check.isin(ALLOWED_REGIONS), nullable=False),
        "quantity": pa.Column(int, checks=pa.Check.gt(0), nullable=False),
        "price": pa.Column(float, checks=pa.Check.ge(0), nullable=False),
        "discount": pa.Column(float, checks=pa.Check.in_range(0, 1), nullable=False),
        "order_status": pa.Column(str, checks=pa.Check.isin(ALLOWED_ORDER_STATUSES), nullable=False),
        "category": pa.Column(str, checks=pa.Check.str_length(min_value=1), nullable=False),
        "brand": pa.Column(str, checks=pa.Check.str_length(min_value=1), nullable=False),
        "rating": pa.Column(float, checks=pa.Check.in_range(1.0, 5.0), nullable=False),
        "in_stock": pa.Column(bool, nullable=False),
        "launch_date": pa.Column(pa.DateTime, nullable=True),
        "gross_revenue": pa.Column(float, checks=pa.Check.ge(0), nullable=False),
        "discount_amount": pa.Column(float, checks=pa.Check.ge(0), nullable=False),
        "net_revenue": pa.Column(float, checks=pa.Check.ge(0), nullable=False),
    },
    checks=[
        pa.Check(_is_not_empty, name="non_empty_dataset"),
        pa.Check(_gross_revenue_is_consistent, name="gross_revenue_is_consistent"),
        pa.Check(_discount_amount_is_consistent, name="discount_amount_is_consistent"),
        pa.Check(_net_revenue_is_consistent, name="net_revenue_is_consistent"),
    ],
    strict=True,
)
