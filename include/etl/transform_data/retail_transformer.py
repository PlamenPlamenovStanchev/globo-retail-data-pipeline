"""Orchestrate pure sales/product transformations and their safe analytical join."""

from dataclasses import dataclass

import pandas as pd

from include.etl.transform_data.products_transformer import transform_products
from include.etl.transform_data.sales_transformer import transform_sales
from include.utils.logger import setup_logger


logger = setup_logger(__name__)
RETAIL_OUTPUT_COLUMNS = [
    "sales_id", "product_id", "timestamp", "region", "quantity", "price", "discount",
    "order_status", "category", "brand", "rating", "in_stock", "launch_date",
    "gross_revenue", "discount_amount", "net_revenue",
]


@dataclass
class RetailTransformationResult:
    """Joined analytical retail rows and source rows rejected during conversion."""

    transformed_rows: pd.DataFrame
    sales_rejected_rows: pd.DataFrame
    products_rejected_rows: pd.DataFrame


def transform_retail(sales_dataframe: pd.DataFrame, products_dataframe: pd.DataFrame) -> RetailTransformationResult:
    """Transform RAW datasets and left-join products onto sales without row multiplication."""
    sales_result = transform_sales(sales_dataframe)
    products_result = transform_products(products_dataframe)
    merged_rows = sales_result.transformed_rows.merge(
        products_result.transformed_rows,
        on="product_id",
        how="left",
        validate="many_to_one",
        sort=False,
    )
    merged_rows = merged_rows[RETAIL_OUTPUT_COLUMNS]
    logger.info(
        "Retail datasets merged: sales_rows=%s output_rows=%s",
        len(sales_result.transformed_rows),
        len(merged_rows),
    )
    return RetailTransformationResult(
        transformed_rows=merged_rows,
        sales_rejected_rows=sales_result.rejected_rows,
        products_rejected_rows=products_result.rejected_rows,
    )
