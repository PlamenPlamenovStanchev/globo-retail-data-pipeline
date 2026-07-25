"""Pandera contract for the transformed retail dataset."""

import pandera.pandas as pa


RETAIL_OUTPUT_SCHEMA = pa.DataFrameSchema(
    {
        "sale_id": pa.Column(str, nullable=False),
        "product_id": pa.Column(str, nullable=False),
        "quantity": pa.Column(int, checks=pa.Check.gt(0), nullable=False),
        "unit_price": pa.Column(float, checks=pa.Check.ge(0), nullable=False),
        "total_amount": pa.Column(float, checks=pa.Check.ge(0), nullable=False),
        "sale_timestamp": pa.Column(pa.DateTime, nullable=False),
    },
    coerce=True,
    strict=False,
)
