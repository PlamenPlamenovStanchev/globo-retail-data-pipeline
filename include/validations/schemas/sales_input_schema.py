"""Lightweight structural contract for the RAW sales CSV."""

import pandera.pandas as pa


SALES_INPUT_SCHEMA: pa.DataFrameSchema = pa.DataFrameSchema(
    {
        "sales id": pa.Column(int, nullable=False),
        "proDuct Id": pa.Column(int, nullable=False),
        "Region": pa.Column(str, nullable=True),
        "qty": pa.Column(int, nullable=False),
        "Price": pa.Column(float, nullable=False),
        "Time stamp": pa.Column(str, nullable=False),
        "discount": pa.Column(float, nullable=False),
        "order_status": pa.Column(str, nullable=False),
    },
    # The RAW source has an exact field list; extra or missing columns are drift.
    # No coercion or business-rule checks are applied at this stage.
    strict=True,
)
