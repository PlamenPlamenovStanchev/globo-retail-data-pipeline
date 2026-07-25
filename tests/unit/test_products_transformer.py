"""Unit tests for pure RAW products transformation."""

import unittest

import pandas as pd

from include.etl.transform_data.products_transformer import transform_products


def _products_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "product_id": ["1000", "1001"],
            "category": [" Sports ", "Home"],
            "brand": [" BrandA ", "BrandB"],
            "rating": ["4.5", 7.0],
            "in_stock": [True, "false"],
            "launch_date": ["2024-01-01", None],
        }
    )


class ProductsTransformerTests(unittest.TestCase):
    """Verify product conversions and representation-based rejection."""

    def test_converts_types_trims_text_and_preserves_null_date(self) -> None:
        result = transform_products(_products_frame())
        transformed = result.transformed_rows

        self.assertEqual(transformed.loc[0, "product_id"], 1000)
        self.assertEqual(transformed.loc[0, "rating"], 4.5)
        self.assertEqual(transformed.loc[0, "category"], "Sports")
        self.assertEqual(transformed.loc[0, "brand"], "BrandA")
        self.assertEqual(transformed.loc[0, "launch_date"], pd.Timestamp("2024-01-01"))
        self.assertTrue(pd.isna(transformed.loc[1, "launch_date"]))
        self.assertFalse(transformed.loc[1, "in_stock"])

    def test_out_of_range_numeric_rating_remains_transformable(self) -> None:
        result = transform_products(_products_frame())

        self.assertEqual(len(result.rejected_rows), 0)
        self.assertEqual(result.transformed_rows.loc[1, "rating"], 7.0)

    def test_malformed_values_are_rejected_once_with_reason(self) -> None:
        dataframe = _products_frame()
        dataframe.loc[0, "product_id"] = "bad-id"
        dataframe.loc[0, "launch_date"] = "not-a-date"
        dataframe.loc[0, "in_stock"] = "maybe"

        result = transform_products(dataframe)

        self.assertEqual(len(result.transformed_rows), 1)
        self.assertEqual(len(result.rejected_rows), 1)
        rejection = result.rejected_rows.iloc[0]
        self.assertIn("product_id: invalid integer", rejection["rejection_reason"])
        self.assertIn("launch_date: invalid date", rejection["rejection_reason"])
        self.assertIn("in_stock: invalid boolean", rejection["rejection_reason"])

    def test_input_dataframe_is_not_modified(self) -> None:
        dataframe = _products_frame()
        original = dataframe.copy(deep=True)

        transform_products(dataframe)

        pd.testing.assert_frame_equal(dataframe, original)
