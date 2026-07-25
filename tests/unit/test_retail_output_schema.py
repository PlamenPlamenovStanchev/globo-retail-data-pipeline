"""Unit tests for the strict transformed retail output schema."""

import unittest

import pandas as pd
import pandera.pandas as pa

from include.validations.schemas.retail_output_schema import RETAIL_OUTPUT_SCHEMA


def _valid_output_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "sales_id": [1],
            "product_id": [1000],
            "timestamp": pd.to_datetime(["2024-01-01 10:00:00"]),
            "region": ["North"],
            "quantity": [2],
            "price": [10.0],
            "discount": [0.1],
            "order_status": ["Completed"],
            "category": ["Sports"],
            "brand": ["BrandA"],
            "rating": [4.5],
            "in_stock": [True],
            "launch_date": pd.to_datetime(["2024-01-01"]),
            "gross_revenue": [20.0],
            "discount_amount": [2.0],
            "net_revenue": [18.0],
        }
    )


class RetailOutputSchemaTests(unittest.TestCase):
    """Verify the final analytical quality gate without storage or Airflow access."""

    def assert_invalid(self, dataframe: pd.DataFrame) -> None:
        """Assert strict lazy validation reports one or more output failures."""
        with self.assertRaises(pa.errors.SchemaErrors):
            RETAIL_OUTPUT_SCHEMA.validate(dataframe, lazy=True)

    def test_valid_transformed_output_passes(self) -> None:
        RETAIL_OUTPUT_SCHEMA.validate(_valid_output_frame(), lazy=True)

    def test_missing_or_unexpected_column_fails(self) -> None:
        self.assert_invalid(_valid_output_frame().drop(columns="price"))
        unexpected = _valid_output_frame()
        unexpected["unexpected"] = "value"
        self.assert_invalid(unexpected)

    def test_wrong_final_type_and_raw_timestamp_string_fail(self) -> None:
        wrong_price = _valid_output_frame()
        wrong_price["price"] = pd.Series(["10.0"], dtype=object)
        self.assert_invalid(wrong_price)
        raw_timestamp = _valid_output_frame()
        raw_timestamp["timestamp"] = pd.Series(["01-01-24 10:00"], dtype=object)
        self.assert_invalid(raw_timestamp)

    def test_identifier_rules_fail_for_null_duplicate_or_non_positive_values(self) -> None:
        for sales_id in (None, 0, -1):
            with self.subTest(sales_id=sales_id):
                dataframe = _valid_output_frame()
                dataframe.loc[0, "sales_id"] = sales_id
                self.assert_invalid(dataframe)
        duplicate = pd.concat([_valid_output_frame(), _valid_output_frame()], ignore_index=True)
        self.assert_invalid(duplicate)
        null_product_id = _valid_output_frame()
        null_product_id.loc[0, "product_id"] = None
        self.assert_invalid(null_product_id)

    def test_sales_business_rules_fail(self) -> None:
        cases = {
            "quantity": 0,
            "price": -1.0,
            "discount": -0.1,
            "order_status": "Unknown",
            "region": None,
        }
        for column, value in cases.items():
            with self.subTest(column=column):
                dataframe = _valid_output_frame()
                dataframe.loc[0, column] = value
                self.assert_invalid(dataframe)
        invalid_region = _valid_output_frame()
        invalid_region.loc[0, "region"] = "Central"
        self.assert_invalid(invalid_region)
        excessive_discount = _valid_output_frame()
        excessive_discount.loc[0, "discount"] = 1.1
        self.assert_invalid(excessive_discount)
        negative_quantity = _valid_output_frame()
        negative_quantity.loc[0, "quantity"] = -1
        self.assert_invalid(negative_quantity)

    def test_product_rules_and_nullable_launch_date(self) -> None:
        null_category = _valid_output_frame()
        null_category.loc[0, "category"] = None
        self.assert_invalid(null_category)
        empty_category = _valid_output_frame()
        empty_category.loc[0, "category"] = ""
        self.assert_invalid(empty_category)
        arbitrary_category = _valid_output_frame()
        arbitrary_category.loc[0, "category"] = "Future Category"
        RETAIL_OUTPUT_SCHEMA.validate(arbitrary_category, lazy=True)
        null_brand = _valid_output_frame()
        null_brand.loc[0, "brand"] = None
        self.assert_invalid(null_brand)
        for rating in (0.9, 5.1):
            with self.subTest(rating=rating):
                dataframe = _valid_output_frame()
                dataframe.loc[0, "rating"] = rating
                self.assert_invalid(dataframe)
        non_boolean = _valid_output_frame()
        non_boolean["in_stock"] = pd.Series(["yes"], dtype=object)
        self.assert_invalid(non_boolean)
        null_launch_date = _valid_output_frame()
        null_launch_date.loc[0, "launch_date"] = pd.NaT
        RETAIL_OUTPUT_SCHEMA.validate(null_launch_date, lazy=True)

    def test_inconsistent_derived_metrics_fail_and_small_tolerance_passes(self) -> None:
        for column, value in (
            ("gross_revenue", 21.0),
            ("discount_amount", 3.0),
            ("net_revenue", 17.0),
        ):
            with self.subTest(column=column):
                dataframe = _valid_output_frame()
                dataframe.loc[0, column] = value
                self.assert_invalid(dataframe)
        tolerance_case = _valid_output_frame()
        tolerance_case.loc[0, "net_revenue"] = 18.0 + 1e-10
        RETAIL_OUTPUT_SCHEMA.validate(tolerance_case, lazy=True)

    def test_missing_joined_product_metadata_and_empty_dataset_fail(self) -> None:
        missing_category = _valid_output_frame()
        missing_category.loc[0, "category"] = None
        self.assert_invalid(missing_category)
        missing_rating = _valid_output_frame()
        missing_rating.loc[0, "rating"] = None
        self.assert_invalid(missing_rating)
        empty = _valid_output_frame().iloc[0:0]
        self.assert_invalid(empty)
