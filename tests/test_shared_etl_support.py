import tempfile
import unittest
from pathlib import Path
from unittest import mock

import etl_blob_upload
from etl_ingestion import BaseSqlServerIngestor


DATABASE_CONFIG = {
    "database": {
        "server": "server",
        "database": "database",
        "username": "username",
        "password": "password",
    }
}

STORAGE_CONFIG = {
    "storage": {
        "connection_string": "UseDevelopmentStorage=true",
        "state_wise_container_name": "raw-state",
        "state_wise_csv_container_name": "csv-state",
    }
}


class RecordingCursor:
    def __init__(self):
        self.executed = []
        self.executemany_calls = []
        self.fast_executemany = False
        self.closed = False

    def execute(self, query):
        self.executed.append(query)

    def executemany(self, query, data):
        self.executemany_calls.append((query, list(data)))

    def close(self):
        self.closed = True


class RecordingConnection:
    def __init__(self):
        self.cursor_instance = RecordingCursor()
        self.commit_count = 0
        self.closed = False

    def cursor(self):
        return self.cursor_instance

    def commit(self):
        self.commit_count += 1

    def close(self):
        self.closed = True


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

    def create_container(self):
        return None


class FakeBlobServiceClient:
    def __init__(self):
        self.container_clients = {}

    def get_container_client(self, name):
        client = self.container_clients.get(name)
        if client is None:
            client = FakeContainerClient()
            self.container_clients[name] = client
        return client


class DummyIngestor(BaseSqlServerIngestor):
    def __init__(self):
        with mock.patch("etl_ingestion.load_config", return_value=DATABASE_CONFIG):
            super().__init__(
                file_prefix="demo_file",
                staging_table_name="staging_demo",
                final_table_name="final_demo",
                merge_key_columns=["date", "state", "vehicle_class"],
                missing_file_hint="demo_preprocessing",
            )


class SharedSqlIngestionTests(unittest.TestCase):
    def test_build_delete_query_uses_all_merge_keys(self):
        ingestor = DummyIngestor()

        query = ingestor.build_delete_query()

        self.assertIn("s.date = final_demo.date", query)
        self.assertIn("s.state = final_demo.state", query)
        self.assertIn("s.vehicle_class = final_demo.vehicle_class", query)

    def test_data_ingest_from_file_executes_shared_load_flow(self):
        ingestor = DummyIngestor()
        connection = RecordingConnection()

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "demo.csv"
            csv_path.write_text(
                "date,state,vehicle_class\n01/06/2026,Telangana,MOTOR CAR\n02/06/2026,,BUS\n",
                encoding="utf-8",
            )

            with mock.patch.object(ingestor, "connect", return_value=connection):
                inserted_rows = ingestor.data_ingest_from_file(csv_path)

        self.assertEqual(inserted_rows, 2)
        self.assertEqual(connection.commit_count, 2)
        self.assertTrue(connection.closed)
        self.assertTrue(connection.cursor_instance.closed)
        self.assertTrue(connection.cursor_instance.fast_executemany)
        self.assertEqual(
            connection.cursor_instance.executed[0],
            "TRUNCATE TABLE staging_demo",
        )
        self.assertIn("DELETE FROM final_demo", connection.cursor_instance.executed[1])
        self.assertIn("INSERT INTO final_demo", connection.cursor_instance.executed[2])
        insert_query, inserted_data = connection.cursor_instance.executemany_calls[0]
        self.assertIn("(date, state, vehicle_class, inserted_at)", insert_query)
        self.assertEqual(
            inserted_data,
            [
                ("01/06/2026", "Telangana", "MOTOR CAR"),
                ("02/06/2026", None, "BUS"),
            ],
        )

    def test_data_ingest_from_file_skips_empty_csv(self):
        ingestor = DummyIngestor()

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "empty.csv"
            csv_path.write_text("date,state,vehicle_class\n", encoding="utf-8")

            with mock.patch.object(ingestor, "connect") as connect_mock:
                inserted_rows = ingestor.data_ingest_from_file(csv_path)

        self.assertEqual(inserted_rows, 0)
        connect_mock.assert_not_called()


class SharedBlobUploadTests(unittest.TestCase):
    def test_upload_pipeline_artifacts_uses_shared_configured_paths(self):
        fake_blob_service = FakeBlobServiceClient()

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            raw_root = root / "state_level" / "state_level_ev_data"
            report_path = raw_root / "Telangana" / "2026" / "JUN" / "reportTable.xlsx"
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_bytes(b"xlsx-bytes")

            csv_path = root / "state_level_ev_data_JUN_2026.csv"
            csv_path.write_text("a,b\n1,2\n", encoding="utf-8")

            with mock.patch.object(
                etl_blob_upload,
                "load_config",
                return_value=STORAGE_CONFIG,
            ):
                with mock.patch.object(
                    etl_blob_upload.BlobServiceClient,
                    "from_connection_string",
                    return_value=fake_blob_service,
                ):
                    raw_count, csv_name = etl_blob_upload.upload_pipeline_artifacts(
                        month="JUN",
                        year="2026",
                        raw_file_pattern=str(
                            raw_root / "*" / "{year}" / "{month}" / "*.xlsx"
                        ),
                        relative_root=str(raw_root),
                        raw_container_config_key="state_wise_container_name",
                        csv_container_config_key="state_wise_csv_container_name",
                        csv_prefix="state_level_ev_data_{month}_{year}",
                        processed_file_directory=root,
                    )

        self.assertEqual(raw_count, 1)
        self.assertEqual(csv_name, "state_level_ev_data_JUN_2026.csv")
        self.assertEqual(
            fake_blob_service.container_clients["raw-state"].uploads,
            [("Telangana/2026/JUN/reportTable.xlsx", b"xlsx-bytes", True)],
        )
        self.assertEqual(
            fake_blob_service.container_clients["csv-state"].uploads,
            [("state_level_ev_data_JUN_2026.csv", b"a,b\n1,2\n", True)],
        )


if __name__ == "__main__":
    unittest.main()
