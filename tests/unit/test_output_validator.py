"""Unit tests for the strict reusable retail output validator."""

import unittest
from unittest.mock import patch

import pandas as pd
import pandera.pandas as pa

from include.exceptions.pipeline_exceptions import OutputValidationError
from include.validations.output_validator import validate_retail_output
from include.validations.schemas.retail_output_schema import RETAIL_OUTPUT_SCHEMA


def _valid_output_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "sales_id": [1], "product_id": [1000],
            "timestamp": pd.to_datetime(["2024-01-01 10:00:00"]), "region": ["North"],
            "quantity": [2], "price": [10.0], "discount": [0.1], "order_status": ["Completed"],
            "category": ["Sports"], "brand": ["BrandA"], "rating": [4.5], "in_stock": [True],
            "launch_date": pd.to_datetime(["2024-01-01"]), "gross_revenue": [20.0],
            "discount_amount": [2.0], "net_revenue": [18.0],
        }
    )


class OutputValidatorTests(unittest.TestCase):
    """Verify strict output validation blocks invalid analytical datasets."""

    def assert_blocked(self, dataframe: pd.DataFrame) -> OutputValidationError:
        """Assert that invalid output raises the project hard-gate exception."""
        with self.assertRaises(OutputValidationError) as context:
            validate_retail_output(dataframe)
        return context.exception

    def test_valid_output_returns_validated_dataframe(self) -> None:
        dataframe = _valid_output_frame()

        result = validate_retail_output(dataframe)

        pd.testing.assert_frame_equal(result, dataframe)

    def test_business_and_join_quality_failures_are_blocked(self) -> None:
        cases = {
            "price": -1.0,
            "quantity": 0,
            "discount": 1.1,
            "rating": 7.0,
            "category": None,
        }
        for column, value in cases.items():
            with self.subTest(column=column):
                dataframe = _valid_output_frame()
                dataframe.loc[0, column] = value
                self.assert_blocked(dataframe)

    def test_incorrect_revenue_duplicate_ids_and_bad_structure_are_blocked(self) -> None:
        incorrect_revenue = _valid_output_frame()
        incorrect_revenue.loc[0, "net_revenue"] = 17.0
        self.assert_blocked(incorrect_revenue)
        duplicate_ids = pd.concat([_valid_output_frame(), _valid_output_frame()], ignore_index=True)
        self.assert_blocked(duplicate_ids)
        malformed = _valid_output_frame().drop(columns="brand")
        self.assert_blocked(malformed)

    def test_empty_output_is_blocked(self) -> None:
        error = self.assert_blocked(_valid_output_frame().iloc[0:0])

        self.assertIn("empty", str(error))

    def test_error_has_summary_and_preserves_pandera_cause(self) -> None:
        dataframe = _valid_output_frame()
        dataframe.loc[0, "price"] = -1.0

        error = self.assert_blocked(dataframe)

        self.assertIn("data-quality violations", str(error))
        self.assertIsInstance(error.__cause__, pa.errors.SchemaErrors)

    def test_non_dataframe_and_unexpected_errors_are_not_swallowed(self) -> None:
        with self.assertRaises(TypeError):
            validate_retail_output("not-a-dataframe")  # type: ignore[arg-type]
        with patch.object(RETAIL_OUTPUT_SCHEMA, "validate", side_effect=RuntimeError("unexpected bug")):
            with self.assertRaisesRegex(RuntimeError, "unexpected bug"):
                validate_retail_output(_valid_output_frame())
