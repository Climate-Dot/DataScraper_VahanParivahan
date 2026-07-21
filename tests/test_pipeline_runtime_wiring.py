import importlib.util
import tempfile
import unittest
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[1]

DATABASE_CONFIG = {
    "database": {
        "server": "server",
        "database": "database",
        "username": "username",
        "password": "password",
    }
}


def load_module(relative_path: str, module_name: str):
    module_path = REPO_ROOT / relative_path
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class PipelineIngestorWiringTests(unittest.TestCase):
    def test_oem_ingestor_uses_expected_tables_and_keys(self):
        module = load_module("oem_level/data_ingestion.py", "oem_ingestion_module")

        with mock.patch("etl_ingestion.load_config", return_value=DATABASE_CONFIG):
            ingestor = module.OEMDataIngest()

        self.assertEqual(ingestor.file_prefix, "oem_data_by_state_and_category")
        self.assertEqual(
            ingestor.staging_table_name,
            "staging_fact_oem_data_by_state_and_category",
        )
        self.assertEqual(
            ingestor.final_table_name,
            "fact_oem_data_by_state_and_category",
        )
        self.assertEqual(ingestor.merge_key_columns, ["date", "state", "vehicle_class"])
        self.assertEqual(
            ingestor.build_file_path("JUL", "2026"),
            "oem_data_by_state_and_category_JUL_2026.csv",
        )

    def test_state_ingestor_uses_expected_tables_and_keys(self):
        module = load_module(
            "state_level/state_level_data_ingestion.py",
            "state_ingestion_module",
        )

        with mock.patch("etl_ingestion.load_config", return_value=DATABASE_CONFIG):
            ingestor = module.StateDataIngest()

        self.assertEqual(ingestor.file_prefix, "state_level_ev_data")
        self.assertEqual(ingestor.staging_table_name, "staging_fact_ev_data_by_state")
        self.assertEqual(ingestor.final_table_name, "fact_ev_data_by_state")
        self.assertEqual(ingestor.merge_key_columns, ["date", "state", "vehicle_class"])
        self.assertEqual(
            ingestor.build_file_path("JUL", "2026"),
            "state_level_ev_data_JUL_2026.csv",
        )

    def test_rto_ingestor_uses_expected_tables_and_keys(self):
        module = load_module("rto_level/rto_level_data_ingestion.py", "rto_ingestion_module")

        with mock.patch("etl_ingestion.load_config", return_value=DATABASE_CONFIG):
            ingestor = module.RtoDataIngest()

        self.assertEqual(ingestor.file_prefix, "rto_level_ev_data")
        self.assertEqual(ingestor.staging_table_name, "staging_fact_ev_data_by_rto")
        self.assertEqual(ingestor.final_table_name, "fact_ev_data_by_rto")
        self.assertEqual(
            ingestor.merge_key_columns,
            ["date", "state", "rto_code", "rto_name", "vehicle_class"],
        )
        self.assertEqual(
            ingestor.build_file_path("JUL", "2026"),
            "rto_level_ev_data_JUL_2026.csv",
        )


class PipelineUploadWrapperTests(unittest.TestCase):
    def test_oem_upload_wrapper_uses_expected_artifact_paths(self):
        module = load_module(
            "oem_level/upload_files_to_blob_storage.py",
            "oem_upload_module",
        )

        with mock.patch.object(module, "resolve_month_year_args", return_value=("JUL", "2026")):
            with mock.patch.object(module, "upload_pipeline_artifacts") as upload_mock:
                module.main()

        upload_mock.assert_called_once_with(
            month="JUL",
            year="2026",
            raw_file_pattern="oem_level/oem_data_by_state_and_category/*/*/{year}/{month}/*.xlsx",
            relative_root="oem_level/oem_data_by_state_and_category",
            raw_container_config_key="container_name",
            csv_container_config_key="csv_container_name",
            csv_prefix="oem_data_by_state_and_category_{month}_{year}",
        )

    def test_state_upload_wrapper_uses_expected_artifact_paths(self):
        module = load_module(
            "state_level/upload_files_to_blob_storage.py",
            "state_upload_module",
        )

        with mock.patch.object(module, "resolve_month_year_args", return_value=("JUL", "2026")):
            with mock.patch.object(module, "upload_pipeline_artifacts") as upload_mock:
                module.main()

        upload_mock.assert_called_once_with(
            month="JUL",
            year="2026",
            raw_file_pattern="state_level/state_level_ev_data/*/{year}/{month}/*.xlsx",
            relative_root="state_level/state_level_ev_data",
            raw_container_config_key="state_wise_container_name",
            csv_container_config_key="state_wise_csv_container_name",
            csv_prefix="state_level_ev_data_{month}_{year}",
        )

    def test_rto_upload_wrapper_uses_expected_artifact_paths(self):
        module = load_module(
            "rto_level/upload_files_to_blob_storage.py",
            "rto_upload_module",
        )

        with mock.patch.object(module, "resolve_month_year_args", return_value=("JUL", "2026")):
            with mock.patch.object(module, "upload_pipeline_artifacts") as upload_mock:
                module.main()

        upload_mock.assert_called_once_with(
            month="JUL",
            year="2026",
            raw_file_pattern="rto_level/rto_level_ev_data/*/*/{year}/{month}/*.xlsx",
            relative_root="rto_level/rto_level_ev_data",
            raw_container_config_key="rto_wise_container_name",
            csv_container_config_key="rto_wise_csv_container_name",
            csv_prefix="rto_level_ev_data_{month}_{year}",
        )


if __name__ == "__main__":
    unittest.main()
