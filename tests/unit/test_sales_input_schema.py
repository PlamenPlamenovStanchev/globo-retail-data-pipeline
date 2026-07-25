"""Unit tests for the RAW sales Pandera schema."""

import unittest

import pandas as pd
import pandera.pandas as pa

from include.validations.schemas.sales_input_schema import SALES_INPUT_SCHEMA


def _valid_sales_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "sales id": [1],
            "proDuct Id": [1000],
            "Region": ["east"],
            "qty": [2],
            "Price": [19.99],
            "Time stamp": ["01-01-24 0:00"],
            "discount": [0.1],
            "order_status": ["Completed"],
        }
    )


class SalesInputSchemaTests(unittest.TestCase):
    """Verify that the sales schema performs structural validation only."""

    def assert_invalid(self, data: pd.DataFrame) -> None:
        """Assert lazy schema validation reports one or more failures."""
        with self.assertRaises(pa.errors.SchemaErrors):
            SALES_INPUT_SCHEMA.validate(data, lazy=True)

    def test_expected_raw_structure_passes(self) -> None:
        SALES_INPUT_SCHEMA.validate(_valid_sales_frame(), lazy=True)

    def test_dirty_business_values_pass(self) -> None:
        data = _valid_sales_frame()
        data.loc[0, "Price"] = -1.0
        data.loc[0, "qty"] = -2
        data.loc[0, "Time stamp"] = "not-a-timestamp"
        data.loc[0, "discount"] = 1.5
        data.loc[0, "order_status"] = "Unexpected state"
        SALES_INPUT_SCHEMA.validate(data, lazy=True)

    def test_null_and_mixed_case_regions_are_allowed(self) -> None:
        data = pd.concat([_valid_sales_frame(), _valid_sales_frame()], ignore_index=True)
        data.loc[0, "Region"] = None
        data.loc[1, "Region"] = "WEST"
        data.loc[1, "sales id"] = 2
        SALES_INPUT_SCHEMA.validate(data, lazy=True)

    def test_missing_required_column_fails(self) -> None:
        data = _valid_sales_frame().drop(columns="sales id")
        self.assert_invalid(data)

    def test_incompatible_column_type_fails(self) -> None:
        data = _valid_sales_frame()
        data["qty"] = pd.Series(["not-a-number"], dtype=object)
        self.assert_invalid(data)
