"""Lightweight structural contract for the RAW products JSON."""

import pandera.pandas as pa


PRODUCTS_INPUT_SCHEMA: pa.DataFrameSchema = pa.DataFrameSchema(
    {
        "product_id": pa.Column(int, nullable=False),
        "category": pa.Column(str, nullable=False),
        "brand": pa.Column(str, nullable=False),
        "rating": pa.Column(float, nullable=False),
        "in_stock": pa.Column(bool, nullable=False),
        "launch_date": pa.Column(str, nullable=True),
    },
    # The RAW source has an exact field list; extra or missing columns are drift.
    # No coercion or business-rule checks are applied at this stage.
    strict=True,
)
