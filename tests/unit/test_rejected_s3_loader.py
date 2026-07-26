"""Unit tests for rejected-record S3 persistence without AWS access."""

from dataclasses import asdict
import unittest
from unittest.mock import patch

import pandas as pd

from include.etl.load_data.rejected_s3_loader import write_rejected_records


TEST_CONFIG = {
    "aws": {"connection_id": "test_aws_connection"},
    "s3": {
        "bucket_name": "test-bucket",
        "rejected_zone": {
            "rejected_sales_key": "project/rejected-zone/rejected_sales.parquet",
            "rejected_products_key": "project/rejected-zone/rejected_products.parquet",
        },
    },
}


class RejectedS3LoaderTests(unittest.TestCase):
    """Verify generated keys and write behavior at the mocked S3 boundary."""

    def setUp(self) -> None:
        self.dataframe = pd.DataFrame(
            {
                "sales id": [1],
                "Price": [-5.0],
                "rejection_reason": ["negative price"],
            }
        )
        self.config_patch = patch(
            "include.etl.load_data.rejected_s3_loader.load_config", return_value=TEST_CONFIG
        )
        self.storage_patch = patch(
            "include.etl.load_data.rejected_s3_loader.get_storage_options",
            return_value={"key": "not-returned"},
        )
        self.mock_config = self.config_patch.start()
        self.mock_storage = self.storage_patch.start()
        self.addCleanup(self.config_patch.stop)
        self.addCleanup(self.storage_patch.stop)

    def _write(self, dataset_name: str):
        return write_rejected_records(self.dataframe, dataset_name)

    def test_sales_key_and_preserved_columns(self) -> None:
        with patch.object(pd.DataFrame, "to_parquet", autospec=True) as write_mock:
            result = self._write("sales")

        self.assertEqual(
            result.key,
            "project/rejected-zone/rejected_sales.parquet",
        )
        self.assertEqual(result.row_count, 1)
        self.assertTrue(result.written)
        self.assertIs(write_mock.call_args.args[0], self.dataframe)
        self.assertEqual(list(write_mock.call_args.args[0].columns), list(self.dataframe.columns))
        self.assertEqual(write_mock.call_args.args[1], f"s3://test-bucket/{result.key}")
        self.assertEqual(write_mock.call_args.kwargs["engine"], "pyarrow")

    def test_products_key_uses_products_prefix(self) -> None:
        with patch.object(pd.DataFrame, "to_parquet", autospec=True):
            result = self._write("products")

        self.assertEqual(
            result.key,
            "project/rejected-zone/rejected_products.parquet",
        )

    def test_empty_dataframe_skips_s3_write(self) -> None:
        empty_dataframe = self.dataframe.iloc[0:0]
        with patch.object(pd.DataFrame, "to_parquet", autospec=True) as write_mock:
            result = write_rejected_records(
                empty_dataframe,
                "sales",
            )

        self.assertFalse(result.written)
        self.assertEqual(result.row_count, 0)
        self.assertIsNone(result.key)
        write_mock.assert_not_called()
        self.mock_storage.assert_not_called()

    def test_unsupported_dataset_is_rejected_clearly(self) -> None:
        with self.assertRaisesRegex(ValueError, "Unsupported rejected dataset"):
            self._write("unknown")

    def test_write_error_is_propagated(self) -> None:
        with patch.object(pd.DataFrame, "to_parquet", autospec=True, side_effect=OSError("S3 unavailable")):
            with self.assertRaisesRegex(OSError, "S3 unavailable"):
                self._write("sales")

    def test_result_never_contains_credentials(self) -> None:
        with patch.object(pd.DataFrame, "to_parquet", autospec=True):
            result = self._write("sales")

        metadata = asdict(result)
        self.assertEqual(set(metadata), {"bucket", "key", "row_count", "written"})
        self.assertNotIn("secret", metadata)
        self.assertNotIn("token", metadata)
