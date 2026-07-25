"""Unit tests for non-blocking RAW input validation."""

import unittest
from unittest.mock import patch

import pandas as pd

from include.validations.input_validator import validate_products_input, validate_sales_input
from include.validations.schemas.sales_input_schema import SALES_INPUT_SCHEMA


def _sales_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "sales id": [1],
            "proDuct Id": [1000],
            "Region": ["North"],
            "qty": [2],
            "Price": [19.99],
            "Time stamp": ["01-01-24 0:00"],
            "discount": [0.1],
            "order_status": ["Completed"],
        }
    )


def _products_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "product_id": [1000],
            "category": ["Sports"],
            "brand": ["BrandA"],
            "rating": [4.5],
            "in_stock": [True],
            "launch_date": ["2024-01-01"],
        }
    )


class InputValidatorTests(unittest.TestCase):
    """Verify that RAW validation is diagnostic and never filters valid input data."""

    def test_valid_sales_returns_original_data_with_success_metadata(self) -> None:
        dataframe = _sales_frame()

        result = validate_sales_input(dataframe)

        self.assertIs(result.dataframe, dataframe)
        self.assertTrue(result.is_valid)
        self.assertEqual(result.error_count, 0)
        self.assertEqual(result.validation_errors, [])

    def test_valid_products_returns_original_data_with_success_metadata(self) -> None:
        dataframe = _products_frame()

        result = validate_products_input(dataframe)

        self.assertIs(result.dataframe, dataframe)
        self.assertTrue(result.is_valid)
        self.assertEqual(result.error_count, 0)
        self.assertEqual(result.validation_errors, [])

    def test_type_failure_is_non_blocking_and_preserves_all_rows(self) -> None:
        dataframe = _sales_frame()
        dataframe["qty"] = pd.Series(["not-an-integer"], dtype=object)

        result = validate_sales_input(dataframe)

        self.assertIs(result.dataframe, dataframe)
        self.assertFalse(result.is_valid)
        self.assertGreater(result.error_count, 0)
        self.assertTrue(result.validation_errors)
        self.assertEqual(len(result.dataframe), 1)

    def test_missing_column_is_non_blocking(self) -> None:
        dataframe = _products_frame().drop(columns="product_id")

        result = validate_products_input(dataframe)

        self.assertIs(result.dataframe, dataframe)
        self.assertFalse(result.is_valid)
        self.assertGreater(result.error_count, 0)
        self.assertTrue(result.validation_errors)
        self.assertTrue(any("product_id" in error for error in result.validation_errors))

    def test_dirty_business_data_remains_valid_at_input_stage(self) -> None:
        sales_data = _sales_frame()
        sales_data.loc[0, "Price"] = -10.0
        sales_data.loc[0, "Region"] = "Unusual Region"
        products_data = _products_frame()
        products_data.loc[0, "rating"] = 99.0

        sales_result = validate_sales_input(sales_data)
        products_result = validate_products_input(products_data)

        self.assertTrue(sales_result.is_valid)
        self.assertTrue(products_result.is_valid)

    def test_validation_does_not_modify_input_dataframe(self) -> None:
        dataframe = _sales_frame()
        original = dataframe.copy(deep=True)
        dataframe["qty"] = pd.Series(["not-an-integer"], dtype=object)
        original["qty"] = pd.Series(["not-an-integer"], dtype=object)

        validate_sales_input(dataframe)

        pd.testing.assert_frame_equal(dataframe, original)

    def test_unexpected_programming_error_propagates(self) -> None:
        dataframe = _sales_frame()

        with patch.object(SALES_INPUT_SCHEMA, "validate", side_effect=RuntimeError("unexpected bug")):
            with self.assertRaisesRegex(RuntimeError, "unexpected bug"):
                validate_sales_input(dataframe)
