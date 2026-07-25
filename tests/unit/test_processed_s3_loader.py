"""Unit tests for processed-zone Parquet persistence without AWS access."""

from dataclasses import asdict
from datetime import date
import unittest
from unittest.mock import patch

import pandas as pd

from include.etl.load_data.processed_s3_loader import write_processed_data


TEST_CONFIG = {
    "aws": {"connection_id": "test_aws_connection"},
    "s3": {
        "bucket_name": "test-bucket",
        "processed_zone": {"sales_clean_prefix": "project/processed-zone/sales-clean"},
    },
}


class ProcessedS3LoaderTests(unittest.TestCase):
    """Verify direct processed writes and deterministic key construction."""

    def setUp(self) -> None:
        self.dataframe = pd.DataFrame({"sales_id": [1], "net_revenue": [18.0]})
        self.config_patch = patch(
            "include.etl.load_data.processed_s3_loader.load_config", return_value=TEST_CONFIG
        )
        self.storage_patch = patch(
            "include.etl.load_data.processed_s3_loader.get_storage_options",
            return_value=(object(), {"secret": "not-returned"}),
        )
        self.config_patch.start()
        self.mock_storage = self.storage_patch.start()
        self.addCleanup(self.config_patch.stop)
        self.addCleanup(self.storage_patch.stop)

    def _write(self, run_id: str | None = "manual:2026+07"):
        return write_processed_data(self.dataframe, date(2026, 7, 25), run_id=run_id)

    def test_generates_configured_partitioned_key_and_metadata(self) -> None:
        with patch.object(pd.DataFrame, "to_parquet", autospec=True) as write_mock:
            result = self._write()

        self.assertEqual(result.bucket, "test-bucket")
        self.assertEqual(
            result.key,
            "project/processed-zone/sales-clean/run_date=2026-07-25/run_id=manual-2026-07/sales_clean.parquet",
        )
        self.assertEqual(result.row_count, 1)
        self.assertEqual(result.format, "parquet")
        self.assertEqual(result.s3_uri, f"s3://test-bucket/{result.key}")
        self.assertEqual(write_mock.call_args.args[1], result.s3_uri)
        self.assertFalse(write_mock.call_args.kwargs["index"])
        self.assertEqual(write_mock.call_args.kwargs["engine"], "pyarrow")
        self.assertEqual(write_mock.call_args.kwargs["compression"], "snappy")

    def test_same_run_is_idempotent_and_different_run_has_different_key(self) -> None:
        with patch.object(pd.DataFrame, "to_parquet", autospec=True):
            first = self._write("scheduled__2026-07-25")
            retry = self._write("scheduled__2026-07-25")
            different_run = self._write("manual__2026-07-25")

        self.assertEqual(first.key, retry.key)
        self.assertNotEqual(first.key, different_run.key)

    def test_no_run_id_uses_daily_default_key(self) -> None:
        with patch.object(pd.DataFrame, "to_parquet", autospec=True):
            result = self._write(None)

        self.assertEqual(
            result.key,
            "project/processed-zone/sales-clean/run_date=2026-07-25/sales_clean.parquet",
        )

    def test_empty_and_non_dataframe_inputs_raise(self) -> None:
        with self.assertRaisesRegex(ValueError, "empty processed"):
            write_processed_data(self.dataframe.iloc[0:0], "2026-07-25")
        with self.assertRaises(TypeError):
            write_processed_data("not-a-dataframe", "2026-07-25")  # type: ignore[arg-type]

    def test_write_error_propagates_and_result_has_no_credentials(self) -> None:
        with patch.object(pd.DataFrame, "to_parquet", autospec=True, side_effect=OSError("S3 unavailable")):
            with self.assertRaisesRegex(OSError, "S3 unavailable"):
                self._write()
        with patch.object(pd.DataFrame, "to_parquet", autospec=True):
            result = self._write()

        metadata = asdict(result)
        self.assertEqual(set(metadata), {"bucket", "key", "row_count", "format", "s3_uri"})
        self.assertNotIn("secret", metadata)
        self.assertNotIn("token", metadata)
