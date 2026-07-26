"""Combine already-transformed sales and products into analytical retail data."""

from dataclasses import dataclass

import pandas as pd

from include.utils.logger import setup_logger


logger = setup_logger(__name__)
RETAIL_OUTPUT_COLUMNS = [
    "sales_id", "product_id", "timestamp", "region", "quantity", "price", "discount",
    "order_status", "category", "brand", "rating", "in_stock", "launch_date",
    "gross_revenue", "discount_amount", "net_revenue",
]
REQUIRED_PRODUCT_COLUMNS = ("category", "brand", "rating", "in_stock")


@dataclass
class RetailTransformationResult:
    """Joined analytical retail rows and sales without a valid product match."""

    transformed_rows: pd.DataFrame
    sales_rejected_rows: pd.DataFrame
    products_rejected_rows: pd.DataFrame


def transform_retail(sales_dataframe: pd.DataFrame, products_dataframe: pd.DataFrame) -> RetailTransformationResult:
    """Left-join transformed products onto transformed sales without row multiplication."""
    merged_rows = sales_dataframe.merge(
        products_dataframe,
        on="product_id",
        how="left",
        validate="many_to_one",
        sort=False,
    )
    missing_product_mask = merged_rows.loc[:, REQUIRED_PRODUCT_COLUMNS].isna().any(axis=1)
    unmatched_sales = merged_rows.loc[missing_product_mask].copy()
    if not unmatched_sales.empty:
        unmatched_sales["rejection_stage"] = "transformation"
        unmatched_sales["rejection_reason"] = "product_id: no valid matching product"
    merged_rows = merged_rows.loc[~missing_product_mask, RETAIL_OUTPUT_COLUMNS].copy()
    logger.info(
        "Retail datasets merged: sales_rows=%s output_rows=%s",
        len(sales_dataframe),
        len(merged_rows),
    )
    return RetailTransformationResult(
        transformed_rows=merged_rows,
        sales_rejected_rows=unmatched_sales,
        products_rejected_rows=pd.DataFrame(),
    )
