"""Shared result types and helpers for pure DataFrame transformations."""

from dataclasses import dataclass

import pandas as pd


@dataclass
class TransformationResult:
    """Rows successfully transformed and source rows rejected during conversion."""

    transformed_rows: pd.DataFrame
    rejected_rows: pd.DataFrame


def require_columns(dataframe: pd.DataFrame, required_columns: tuple[str, ...], dataset_name: str) -> None:
    """Raise a clear error when an expected RAW source column is absent."""
    missing_columns = [column for column in required_columns if column not in dataframe.columns]
    if missing_columns:
        raise ValueError(f"{dataset_name} transformation missing required columns: {missing_columns}")


def add_rejection_reason(reasons: pd.Series, failed_mask: pd.Series, reason: str) -> None:
    """Append one deterministic reason for every failed source row."""
    existing_reasons = reasons.loc[failed_mask]
    reasons.loc[failed_mask] = existing_reasons.mask(
        existing_reasons.eq(""), ""
    ).where(existing_reasons.eq(""), existing_reasons + "; ") + reason


def build_rejected_rows(source_data: pd.DataFrame, reasons: pd.Series) -> pd.DataFrame:
    """Return original rejected source rows with transformation diagnostics."""
    rejected_rows = source_data.loc[reasons.ne("")].copy()
    rejected_rows["rejection_stage"] = "transformation"
    rejected_rows["rejection_reason"] = reasons.loc[rejected_rows.index]
    return rejected_rows
