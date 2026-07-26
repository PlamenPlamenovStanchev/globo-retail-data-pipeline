"""Structural tests for retail Airflow TaskGroups without executing ETL work."""

import unittest

from airflow.sdk import DAG
from pendulum import datetime

from include.pipelines.retail_pipeline import load_processed_task, validate_output_task
from include.pipelines.task_groups import extract_group, input_validation_group, transform_group


class TaskGroupTests(unittest.TestCase):
    """Verify logical groups, parallelism, and dependencies through DAG inspection."""

    def setUp(self) -> None:
        with DAG(
            dag_id="task_group_structure_test",
            start_date=datetime(2026, 7, 23),
            schedule=None,
            catchup=False,
        ) as dag:
            extracted = extract_group()
            input_validated = input_validation_group(extracted)
            transformed = transform_group(input_validated)
            validated_output = validate_output_task(transformed)
            self.result = load_processed_task(validated_output)
        self.dag = dag

    def test_group_ids_and_task_ids_are_present(self) -> None:
        self.assertTrue(
            {"extract", "input_validation", "transform"}.issubset(self.dag.task_group_dict)
        )
        self.assertEqual(
            set(self.dag.task_ids),
            {
                "extract.extract_sales", "extract.extract_products",
                "input_validation.validate_sales_input", "input_validation.validate_products_input",
                "transform.transform_sales", "transform.transform_products", "transform.transform_retail",
                "validate_output",
                "load_processed",
            },
        )

    def test_dependencies_preserve_parallelism_and_stage_order(self) -> None:
        sales_extract = self.dag.get_task("extract.extract_sales")
        products_extract = self.dag.get_task("extract.extract_products")
        sales_validate = self.dag.get_task("input_validation.validate_sales_input")
        products_validate = self.dag.get_task("input_validation.validate_products_input")
        sales_transform = self.dag.get_task("transform.transform_sales")
        products_transform = self.dag.get_task("transform.transform_products")
        transform = self.dag.get_task("transform.transform_retail")
        output_validate = self.dag.get_task("validate_output")
        load = self.dag.get_task("load_processed")

        self.assertFalse(sales_extract.upstream_task_ids)
        self.assertFalse(products_extract.upstream_task_ids)
        self.assertEqual(sales_validate.upstream_task_ids, {sales_extract.task_id})
        self.assertEqual(products_validate.upstream_task_ids, {products_extract.task_id})
        self.assertEqual(sales_transform.upstream_task_ids, {sales_validate.task_id})
        self.assertEqual(products_transform.upstream_task_ids, {products_validate.task_id})
        self.assertEqual(transform.upstream_task_ids, {sales_transform.task_id, products_transform.task_id})
        self.assertEqual(output_validate.upstream_task_ids, {transform.task_id})
        self.assertEqual(load.upstream_task_ids, {output_validate.task_id})

    def test_group_outputs_are_xcom_arguments_not_dataframes(self) -> None:
        self.assertFalse(hasattr(self.result, "columns"))
