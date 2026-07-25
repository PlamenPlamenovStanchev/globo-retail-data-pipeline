"""Helpers for stable, safe identifiers used in S3 object keys."""

from datetime import date, datetime
import re


def normalise_run_date(run_date: date | str) -> str:
    """Return a validated ISO-8601 date string for a pipeline run."""
    if isinstance(run_date, datetime):
        return run_date.date().isoformat()
    if isinstance(run_date, date):
        return run_date.isoformat()
    if isinstance(run_date, str):
        try:
            return date.fromisoformat(run_date).isoformat()
        except ValueError as error:
            raise ValueError("run_date must be an ISO-8601 date string.") from error
    raise TypeError("run_date must be a date or ISO-8601 date string.")


def sanitise_run_id(run_id: str) -> str:
    """Return a safe, deterministic S3-key segment for an Airflow run ID."""
    if not isinstance(run_id, str):
        raise TypeError("run_id must be a string.")
    sanitised_run_id = re.sub(r"[^A-Za-z0-9_.-]+", "-", run_id).strip(".-")
    if not sanitised_run_id:
        raise ValueError("run_id must contain at least one safe key character.")
    return sanitised_run_id
