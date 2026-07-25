"""Unit tests for pure RAW sales transformation."""

import unittest

import pandas as pd

from include.etl.transform_data.sales_transformer import transform_sales


def _sales_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "sales id": [1, 2],
            "proDuct Id": [1000, 1001],
            "Region": [" west ", " north "],
            "qty": [2, 1],
            "Price": [10.0, 25.0],
            "Time stamp": ["01-01-24 4:05", "02-01-24 5:10"],
            "discount": [0.1, 0.2],
            "order_status": [" completed ", "RETURNED"],
        }
    )


class SalesTransformerTests(unittest.TestCase):
    """Verify explicit sales standardization and representation-based rejection."""

    def test_normalizes_columns_text_types_timestamps_and_revenue(self) -> None:
        dataframe = _sales_frame()
        result = transform_sales(dataframe)
        transformed = result.transformed_rows

        self.assertEqual(
            list(transformed.columns),
            [
                "sales_id", "product_id", "timestamp", "region", "quantity", "price", "discount",
                "order_status", "gross_revenue", "discount_amount", "net_revenue",
            ],
        )
        self.assertEqual(transformed.loc[0, "region"], "West")
        self.assertEqual(transformed.loc[1, "region"], "North")
        self.assertEqual(transformed.loc[0, "order_status"], "Completed")
        self.assertEqual(transformed.loc[0, "timestamp"], pd.Timestamp("2024-01-01 04:05"))
        self.assertEqual(transformed.loc[0, "gross_revenue"], 20.0)
        self.assertEqual(transformed.loc[0, "discount_amount"], 2.0)
        self.assertEqual(transformed.loc[0, "net_revenue"], 18.0)

    def test_business_invalid_values_are_rejected_without_being_changed(self) -> None:
        dataframe = _sales_frame()
        dataframe.loc[1, ["qty", "Price", "discount"]] = [-1, -25.0, 1.2]
        result = transform_sales(dataframe)

        self.assertEqual(len(result.transformed_rows), 1)
        self.assertEqual(len(result.rejected_rows), 1)
        self.assertIn("quantity: must be greater than zero", result.rejected_rows.iloc[0]["rejection_reason"])
        self.assertIn("price: must be non-negative", result.rejected_rows.iloc[0]["rejection_reason"])
        self.assertIn("discount: must be between zero and one", result.rejected_rows.iloc[0]["rejection_reason"])

    def test_multiple_conversion_failures_create_one_rejected_row(self) -> None:
        dataframe = _sales_frame()
        dataframe["qty"] = pd.Series(["not-a-number", 1], dtype=object)
        dataframe.loc[0, "Time stamp"] = "bad-date"

        result = transform_sales(dataframe)

        self.assertEqual(len(result.transformed_rows), 1)
        self.assertEqual(len(result.rejected_rows), 1)
        rejection = result.rejected_rows.iloc[0]
        self.assertEqual(rejection["rejection_stage"], "transformation")
        self.assertIn("quantity: invalid integer", rejection["rejection_reason"])
        self.assertIn("timestamp: invalid datetime", rejection["rejection_reason"])

    def test_input_dataframe_is_not_modified(self) -> None:
        dataframe = _sales_frame()
        original = dataframe.copy(deep=True)

        transform_sales(dataframe)

        pd.testing.assert_frame_equal(dataframe, original)
