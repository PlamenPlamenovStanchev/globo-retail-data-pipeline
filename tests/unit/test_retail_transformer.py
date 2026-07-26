"""Unit tests for safe sales-to-products retail transformation."""

import unittest

import pandas as pd

from include.etl.transform_data.retail_transformer import RETAIL_OUTPUT_COLUMNS, transform_retail
from include.etl.transform_data.products_transformer import transform_products
from include.etl.transform_data.sales_transformer import transform_sales


def _sales_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "sales id": [1, 2], "proDuct Id": [1000, 9999], "Region": ["east", "West"],
            "qty": [2, 1], "Price": [10.0, 5.0], "Time stamp": ["01-01-24 0:00", "02-01-24 0:00"],
            "discount": [0.1, 0.0], "order_status": ["Completed", "Pending"],
        }
    )


def _products_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "product_id": [1000], "category": ["Sports"], "brand": ["BrandA"], "rating": [4.0],
            "in_stock": [True], "launch_date": ["2024-01-01"],
        }
    )


class RetailTransformerTests(unittest.TestCase):
    """Verify many-to-one left joining preserves transactional sales rows."""

    def test_join_rejects_sales_without_a_valid_matching_product(self) -> None:
        result = transform_retail(transform_sales(_sales_frame()).transformed_rows, transform_products(_products_frame()).transformed_rows)
        transformed = result.transformed_rows

        self.assertEqual(len(transformed), 1)
        self.assertEqual(list(transformed.columns), RETAIL_OUTPUT_COLUMNS)
        self.assertEqual(transformed.loc[0, "category"], "Sports")
        self.assertEqual(transformed.loc[0, "gross_revenue"], 20.0)
        self.assertEqual(len(result.sales_rejected_rows), 1)
        self.assertIn("no valid matching product", result.sales_rejected_rows.iloc[0]["rejection_reason"])
        self.assertEqual(len(result.products_rejected_rows), 0)

    def test_duplicate_product_ids_are_rejected_instead_of_multiplying_sales(self) -> None:
        products = pd.concat([_products_frame(), _products_frame()], ignore_index=True)

        result = transform_retail(transform_sales(_sales_frame()).transformed_rows, transform_products(products).transformed_rows)

        self.assertEqual(len(result.transformed_rows), 0)
        self.assertEqual(len(result.products_rejected_rows), 0)
