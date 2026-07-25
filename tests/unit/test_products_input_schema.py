"""Unit tests for the RAW products Pandera schema."""

import unittest

import pandas as pd
import pandera.pandas as pa

from include.validations.schemas.products_input_schema import PRODUCTS_INPUT_SCHEMA


def _valid_products_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "product_id": [1000],
            "category": ["Sports"],
            "brand": ["IndependentBrand"],
            "rating": [4.5],
            "in_stock": [True],
            "launch_date": ["2024-01-01"],
        }
    )


class ProductsInputSchemaTests(unittest.TestCase):
    """Verify that the products schema performs structural validation only."""

    def assert_invalid(self, data: pd.DataFrame) -> None:
        """Assert lazy schema validation reports one or more failures."""
        with self.assertRaises(pa.errors.SchemaErrors):
            PRODUCTS_INPUT_SCHEMA.validate(data, lazy=True)

    def test_expected_raw_structure_passes(self) -> None:
        PRODUCTS_INPUT_SCHEMA.validate(_valid_products_frame(), lazy=True)

    def test_dirty_but_structurally_compatible_values_pass(self) -> None:
        data = _valid_products_frame()
        data.loc[0, "rating"] = 99.0
        data.loc[0, "category"] = "AnyCategory"
        data.loc[0, "brand"] = "Any Brand"
        data.loc[0, "launch_date"] = None
        PRODUCTS_INPUT_SCHEMA.validate(data, lazy=True)

    def test_missing_required_column_fails(self) -> None:
        data = _valid_products_frame().drop(columns="product_id")
        self.assert_invalid(data)

    def test_incompatible_column_type_fails(self) -> None:
        data = _valid_products_frame()
        data["rating"] = pd.Series(["not-a-number"], dtype=object)
        self.assert_invalid(data)

    def test_arbitrary_category_and_brand_pass(self) -> None:
        data = _valid_products_frame()
        data.loc[0, "category"] = "Sports"
        data.loc[0, "brand"] = "Unlisted Brand"
        PRODUCTS_INPUT_SCHEMA.validate(data, lazy=True)
