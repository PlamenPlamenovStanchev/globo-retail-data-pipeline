"""Unit tests for Airflow TaskFlow wrappers without a DAG or AWS connection."""

from dataclasses import asdict
import json
import unittest
from unittest.mock import ANY, patch

import pandas as pd

from include.etl.load_data.processed_s3_loader import ProcessedWriteResult
from include.etl.load_data.rejected_s3_loader import S3WriteResult
from include.etl.transform_data.retail_transformer import RetailTransformationResult
from include.exceptions.pipeline_exceptions import OutputValidationError
from include.pipelines import retail_pipeline
from include.validations.input_validator import InputValidationResult


REFERENCE: retail_pipeline.S3DatasetReference = {
    "bucket": "test-bucket",
    "key": "project/work/extracted/sales/run_date=2026-07-25/run_id=test/sales.parquet",
    "dataset": "sales",
    "row_count": 1,
    "run_date": "2026-07-25",
    "run_id": "test-run",
}


class AirflowTaskTests(unittest.TestCase):
    """Verify task wrappers orchestrate pure functions with metadata-only XCom payloads."""

    def test_extract_validate_task_calls_extractor_and_returns_json_reference(self) -> None:
        dataframe = pd.DataFrame({"sales id": [1]})
        validation_result = InputValidationResult(dataframe, True, 0, [])
        output_reference = {**REFERENCE, "key": "project/work/extracted/sales/output.parquet"}
        with (
            patch.object(retail_pipeline, "_task_run_identity", return_value=("2026-07-25", "test-run")),
            patch.object(retail_pipeline, "extract_sales", return_value=dataframe) as extract_mock,
            patch.object(retail_pipeline, "validate_sales_input", return_value=validation_result),
            patch.object(retail_pipeline, "_write_intermediate_dataframe", return_value=output_reference) as write_mock,
        ):
            result = retail_pipeline.extract_validate_sales_task.function()

        extract_mock.assert_called_once_with()
        write_mock.assert_called_once_with(dataframe, "sales", "extracted", "2026-07-25", "test-run")
        self.assertNotIsInstance(result, pd.DataFrame)
        json.dumps(result)

    def test_extract_validation_stays_non_blocking_and_returns_metadata(self) -> None:
        dataframe = pd.DataFrame({"qty": ["bad"]})
        validation_result = InputValidationResult(dataframe, False, 1, ["Column 'qty' failed dtype"])
        with (
            patch.object(retail_pipeline, "_task_run_identity", return_value=("2026-07-25", "test-run")),
            patch.object(retail_pipeline, "extract_sales", return_value=dataframe),
            patch.object(retail_pipeline, "validate_sales_input", return_value=validation_result),
            patch.object(retail_pipeline, "_write_intermediate_dataframe", return_value=REFERENCE),
        ):
            result = retail_pipeline.extract_validate_sales_task.function()

        self.assertFalse(result["input_validation"]["is_valid"])
        self.assertEqual(result["input_validation"]["issue_count"], 1)
        self.assertNotIn("dataframe", result)
        json.dumps(result)

    def test_strict_output_validation_exception_propagates(self) -> None:
        with (
            patch.object(retail_pipeline, "_read_intermediate_dataframe", return_value=pd.DataFrame()),
            patch.object(retail_pipeline, "validate_retail_output", side_effect=OutputValidationError("quality gate failed")),
        ):
            with self.assertRaisesRegex(OutputValidationError, "quality gate failed"):
                retail_pipeline.validate_output_task.function(REFERENCE)

    def test_transform_task_persists_rejections_and_returns_only_references(self) -> None:
        products_reference = {**REFERENCE, "dataset": "products", "key": "products.parquet"}
        retail_result = RetailTransformationResult(
            transformed_rows=pd.DataFrame({"sales_id": [1]}),
            sales_rejected_rows=pd.DataFrame({"sales id": [2], "rejection_reason": ["bad"]}),
            products_rejected_rows=pd.DataFrame(),
        )
        transformed_reference = {**REFERENCE, "dataset": "retail", "key": "retail.parquet"}
        sales_write = S3WriteResult("test-bucket", "rejected/sales.parquet", 1, True)
        products_write = S3WriteResult("test-bucket", None, 0, False)
        with (
            patch.object(retail_pipeline, "_read_intermediate_dataframe", side_effect=[pd.DataFrame(), pd.DataFrame()]),
            patch.object(retail_pipeline, "transform_retail", return_value=retail_result),
            patch.object(retail_pipeline, "write_rejected_records", side_effect=[sales_write, products_write]) as rejected_mock,
            patch.object(retail_pipeline, "_write_intermediate_dataframe", return_value=transformed_reference),
        ):
            result = retail_pipeline.transform_retail_task.function(REFERENCE, products_reference)

        self.assertEqual(rejected_mock.call_count, 2)
        self.assertEqual(result["transformed"], transformed_reference)
        self.assertEqual(result["sales_rejected"], asdict(sales_write))
        self.assertNotIn("dataframe", result)
        json.dumps(result)

    def test_processed_loader_metadata_is_returned_without_dataframe(self) -> None:
        write_result = ProcessedWriteResult(
            bucket="test-bucket",
            key="project/processed/sales_clean.parquet",
            row_count=1,
            format="parquet",
            s3_uri="s3://test-bucket/project/processed/sales_clean.parquet",
        )
        with (
            patch.object(retail_pipeline, "_read_intermediate_dataframe", return_value=pd.DataFrame({"sales_id": [1]})),
            patch.object(retail_pipeline, "write_processed_data", return_value=write_result) as write_mock,
        ):
            result = retail_pipeline.load_processed_task.function(REFERENCE)

        write_mock.assert_called_once_with(ANY)
        self.assertEqual(result, asdict(write_result))
        self.assertNotIn("dataframe", result)
        json.dumps(result)
