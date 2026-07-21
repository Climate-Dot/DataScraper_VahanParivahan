import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

import blob_storage_utils
import runtime_config
import sqlserver_utils


class FakeBlobClient:
    def __init__(self, blob_name, uploads):
        self.blob_name = blob_name
        self.uploads = uploads

    def upload_blob(self, data, overwrite):
        self.uploads.append((self.blob_name, data.read(), overwrite))


class FakeContainerClient:
    def __init__(self):
        self.uploads = []

    def get_blob_client(self, blob):
        return FakeBlobClient(blob, self.uploads)


class RuntimeConfigTests(unittest.TestCase):
    def test_get_previous_month_year_label_uses_previous_calendar_month(self):
        month, year = runtime_config.get_previous_month_year_label(
            datetime(2026, 7, 17)
        )

        self.assertEqual((month, year), ("JUN", "2026"))

    def test_resolve_month_year_args_normalizes_month_argument(self):
        month, year = runtime_config.resolve_month_year_args(["jul", 2025])

        self.assertEqual((month, year), ("JUL", "2025"))

    def test_load_config_resolves_relative_path_from_project_root_when_cwd_differs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            temp_root = Path(tmpdir)
            project_root = temp_root / "project"
            workspace_root = temp_root / "workspace"
            project_root.mkdir(parents=True, exist_ok=True)
            workspace_root.mkdir(parents=True, exist_ok=True)
            config_path = project_root / "config.yaml"
            config_path.write_text("database:\n  server: test-server\n", encoding="utf-8")

            with mock.patch.object(runtime_config, "PROJECT_ROOT", project_root):
                with mock.patch("runtime_config.Path.cwd", return_value=workspace_root):
                    payload = runtime_config.load_config("config.yaml")

        self.assertEqual(payload["database"]["server"], "test-server")


class SqlServerUtilsTests(unittest.TestCase):
    def test_connect_with_retry_eventually_returns_connection(self):
        calls = []

        def connect():
            calls.append("attempt")
            if len(calls) < 3:
                raise ValueError("boom")
            return "connected"

        result = sqlserver_utils.connect_with_retry(
            connect,
            attempts=3,
            base_wait_seconds=0,
            sleep_func=lambda _: None,
            connection_error_types=(ValueError,),
        )

        self.assertEqual(result, "connected")
        self.assertEqual(len(calls), 3)

    def test_connect_with_retry_raises_last_error_after_exhaustion(self):
        calls = []

        def connect():
            calls.append("attempt")
            raise ValueError("still broken")

        with self.assertRaisesRegex(ValueError, "still broken"):
            sqlserver_utils.connect_with_retry(
                connect,
                attempts=2,
                base_wait_seconds=0,
                sleep_func=lambda _: None,
                connection_error_types=(ValueError,),
            )

        self.assertEqual(len(calls), 2)


class BlobStorageUtilsTests(unittest.TestCase):
    def test_upload_globbed_files_to_container_uploads_relative_paths_and_cleans_up(self):
        container_client = FakeContainerClient()

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "oem_level" / "oem_data_by_state_and_category"
            target_dir = root / "West Bengal" / "MOTOR CAR" / "2026" / "JUN"
            target_dir.mkdir(parents=True)
            report_path = target_dir / "reportTable.xlsx"
            report_path.write_bytes(b"excel-bytes")

            uploaded = blob_storage_utils.upload_globbed_files_to_container(
                [str(report_path)],
                relative_root=str(root),
                container_client=container_client,
            )

            self.assertEqual(uploaded, 1)
            self.assertEqual(
                container_client.uploads,
                [("West Bengal/MOTOR CAR/2026/JUN/reportTable.xlsx", b"excel-bytes", True)],
            )
            self.assertFalse(target_dir.exists())

    def test_upload_matching_csv_artifact_uploads_and_deletes_csv(self):
        container_client = FakeContainerClient()

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "rto_level_ev_data_JUN_2026.csv"
            csv_path.write_text("a,b\n1,2\n", encoding="utf-8")

            uploaded_name = blob_storage_utils.upload_matching_csv_artifact(
                processed_file_directory=tmpdir,
                csv_prefix="rto_level_ev_data_JUN_2026",
                csv_container_client=container_client,
                csv_container_name="rto-csv",
            )

            self.assertEqual(uploaded_name, "rto_level_ev_data_JUN_2026.csv")
            self.assertEqual(
                container_client.uploads,
                [("rto_level_ev_data_JUN_2026.csv", b"a,b\n1,2\n", True)],
            )
            self.assertFalse(csv_path.exists())


if __name__ == "__main__":
    unittest.main()
