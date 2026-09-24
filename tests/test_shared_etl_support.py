import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pandas as pd

import etl_blob_upload
import etl_ingestion
from etl_ingestion import BaseSqlServerIngestor
from etl_preprocessing import BaseExcelPreprocessor


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
        self.execute_params = []
        self.executemany_calls = []
        self.fast_executemany = False
        self.closed = False

    def execute(self, query, params=None):
        self.executed.append(query)
        if params is not None:
            self.execute_params.append(list(params))

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

    def test_build_delete_query_can_replace_a_complete_date_snapshot(self):
        ingestor = DummyIngestor()

        query = ingestor.build_delete_query(["date"])

        self.assertIn("s.date = final_demo.date", query)
        self.assertNotIn("s.state = final_demo.state", query)
        self.assertNotIn("s.vehicle_class = final_demo.vehicle_class", query)

    def test_build_delete_query_can_replace_a_partial_state_snapshot(self):
        ingestor = DummyIngestor()

        query = ingestor.build_delete_query(["date", "state"])

        self.assertIn("s.date = final_demo.date", query)
        self.assertIn("s.state = final_demo.state", query)
        self.assertNotIn("s.vehicle_class = final_demo.vehicle_class", query)

    def test_monthly_ingest_replaces_the_complete_date_snapshot(self):
        ingestor = DummyIngestor()

        with mock.patch.object(
            ingestor,
            "data_ingest_from_file",
            return_value=3,
        ) as ingest_from_file:
            inserted_rows = ingestor.data_ingest("JUN", "2026")

        self.assertEqual(inserted_rows, 3)
        ingest_from_file.assert_called_once_with(
            "demo_file_JUN_2026.csv",
            replacement_scope_columns=["date"],
        )

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

    def test_staging_load_is_chunked(self):
        """One giant executemany hangs on wide tables.

        pyodbc's fast_executemany builds a single parameter array per call, so
        144,182 rows x 51 columns = 7.35M parameters hung indefinitely — 34
        minutes, no rows landed. V1's ~16k-row loads never reached that size.
        """
        ingestor = DummyIngestor()
        connection = RecordingConnection()
        rows = 1 + 2 * etl_ingestion.INSERT_CHUNK_SIZE

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "big.csv"
            with csv_path.open("w", encoding="utf-8") as f:
                f.write("date,state,vehicle_class\n")
                for i in range(rows):
                    f.write(f"01/06/2026,Telangana,CLASS{i}\n")

            with mock.patch.object(ingestor, "connect", return_value=connection):
                inserted = ingestor.data_ingest_from_file(csv_path)

        self.assertEqual(inserted, rows)
        calls = connection.cursor_instance.executemany_calls
        self.assertEqual(len(calls), 3, "expected one call per chunk")
        self.assertEqual([len(c[1]) for c in calls],
                         [etl_ingestion.INSERT_CHUNK_SIZE,
                          etl_ingestion.INSERT_CHUNK_SIZE, 1])
        # Every row still lands exactly once, in order.
        staged = [row for _, chunk in calls for row in chunk]
        self.assertEqual(len(staged), rows)
        self.assertEqual(staged[0][2], "CLASS0")
        self.assertEqual(staged[-1][2], f"CLASS{rows - 1}")
        # Still one commit for the staging load, so semantics are unchanged.
        self.assertEqual(connection.commit_count, 2)

    def test_multirow_insert_packs_rows_under_the_parameter_ceiling(self):
        """SQL Server rejects >2100 parameters, and exactly 2100 also fails."""
        self.assertEqual(etl_ingestion.rows_per_multirow_statement(50), 41)
        self.assertLess(41 * 50, etl_ingestion.MAX_STATEMENT_PARAMETERS)
        self.assertEqual(etl_ingestion.rows_per_multirow_statement(3), 696)
        # A table wider than the ceiling still makes progress, one row at a time.
        self.assertEqual(etl_ingestion.rows_per_multirow_statement(5000), 1)

    def test_multirow_path_is_opt_in_and_v1_is_unaffected(self):
        # V1 loads ~16k rows a month on a path that has worked for years.
        self.assertFalse(DummyIngestor().use_multirow_insert)

    def test_multirow_load_sends_every_row_once_in_order(self):
        ingestor = DummyIngestor()
        ingestor.use_multirow_insert = True
        connection = RecordingConnection()
        rows = 100

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "multi.csv"
            with csv_path.open("w", encoding="utf-8") as f:
                f.write("date,state,vehicle_class\n")
                for i in range(rows):
                    f.write(f"01/06/2026,,CLASS{i}\n")
            with mock.patch.object(ingestor, "connect", return_value=connection):
                inserted = ingestor.data_ingest_from_file(csv_path)

        self.assertEqual(inserted, rows)
        # No executemany at all on this path.
        self.assertEqual(connection.cursor_instance.executemany_calls, [])
        inserts = [q for q in connection.cursor_instance.executed
                   if q.startswith("INSERT INTO staging_demo")]
        self.assertTrue(inserts)
        # Empty CSV cells still become NULL, exactly as on the executemany path.
        params = connection.cursor_instance.execute_params
        flat = [v for p in params for v in p]
        self.assertEqual(flat.count(None), rows)
        self.assertEqual(len([v for v in flat if v == "CLASS0"]), 1)
        self.assertEqual(len([v for v in flat if v == f"CLASS{rows - 1}"]), 1)

    def test_a_small_load_still_uses_a_single_call(self):
        ingestor = DummyIngestor()
        connection = RecordingConnection()
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "small.csv"
            csv_path.write_text(
                "date,state,vehicle_class\n01/06/2026,Telangana,MOTOR CAR\n",
                encoding="utf-8",
            )
            with mock.patch.object(ingestor, "connect", return_value=connection):
                ingestor.data_ingest_from_file(csv_path)
        self.assertEqual(len(connection.cursor_instance.executemany_calls), 1)

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
                    etl_blob_upload, "BlobServiceClient"
                ) as blob_service_cls:
                    blob_service_cls.from_connection_string.return_value = (
                        fake_blob_service
                    )
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


class SharedPreprocessingNumericCoercionTests(unittest.TestCase):
    def test_partially_missing_fuel_column_stays_integer_not_float(self):
        # Simulates concatenating two offices' monthly reports where one
        # office's Excel report simply doesn't have the bio_cng_bio_gas
        # column at all - pandas fills those rows with NaN on concat and
        # upcasts the whole column to float64, which previously rendered
        # as "12.0" in the output CSV and broke the SQL side's CAST(...AS INT).
        office_with_column = pd.DataFrame({"bio_cng_bio_gas": [0, 5], "petrol": [10, 20]})
        office_without_column = pd.DataFrame({"petrol": [7]})
        combined = pd.concat([office_with_column, office_without_column], ignore_index=True)
        self.assertEqual(combined["bio_cng_bio_gas"].dtype, "float64")

        fixed = BaseExcelPreprocessor._coerce_numeric_output_columns(combined)

        self.assertEqual(str(fixed["bio_cng_bio_gas"].dtype), "Int64")
        csv_output = fixed.to_csv(index=False)
        self.assertNotIn(".0", csv_output)
        self.assertEqual(csv_output, "bio_cng_bio_gas,petrol\n0,10\n5,20\n,7\n")

    def test_comma_formatted_numeric_strings_are_stripped(self):
        df = pd.DataFrame({"total": ["1,234", "56"]})

        fixed = BaseExcelPreprocessor._coerce_numeric_output_columns(df)

        self.assertEqual(list(fixed["total"]), [1234, 56])

    def test_mixed_numeric_objects_are_not_lost_during_string_cleanup(self):
        df = pd.DataFrame(
            {
                "pure_ev": pd.Series(
                    [148, "1,234", None],
                    dtype=object,
                )
            }
        )

        fixed = BaseExcelPreprocessor._coerce_numeric_output_columns(df)

        self.assertEqual(str(fixed["pure_ev"].dtype), "Int64")
        self.assertEqual(fixed["pure_ev"].iloc[0], 148)
        self.assertEqual(fixed["pure_ev"].iloc[1], 1234)
        self.assertTrue(pd.isna(fixed["pure_ev"].iloc[2]))

    def test_missing_column_is_untouched(self):
        df = pd.DataFrame({"petrol": [1, 2]})

        fixed = BaseExcelPreprocessor._coerce_numeric_output_columns(df)

        self.assertNotIn("bio_cng_bio_gas", fixed.columns)


class SharedPreprocessingTotalColumnTests(unittest.TestCase):
    def build_processor(self):
        processor = object.__new__(BaseExcelPreprocessor)
        processor.pipeline_label = "test"
        processor.column_rename_map = {"Unnamed: 38": "total"}
        return processor

    def test_legacy_positional_total_is_normalized(self):
        processor = self.build_processor()
        df = pd.DataFrame(
            {
                "Unnamed: 1": ["MOTOR CAR"],
                "DIESEL": [10],
                "Unnamed: 26": [10],
            }
        )

        fixed = processor._normalize_report_total_column(df)

        self.assertEqual(
            list(fixed.columns),
            ["Unnamed: 1", "DIESEL", "Unnamed: 38"],
        )
        self.assertEqual(fixed.loc[0, "Unnamed: 38"], 10)

    def test_current_total_column_is_unchanged(self):
        processor = self.build_processor()
        df = pd.DataFrame(
            {
                "Unnamed: 1": ["MOTOR CAR"],
                "DIESEL": [10],
                "Unnamed: 38": [10],
            }
        )

        fixed = processor._normalize_report_total_column(df)

        self.assertIs(fixed, df)

    def test_unrecognized_total_layout_fails_instead_of_emitting_null_totals(self):
        processor = self.build_processor()
        df = pd.DataFrame(
            {
                "Unnamed: 1": ["MOTOR CAR"],
                "DIESEL": [10],
            }
        )

        with self.assertRaisesRegex(ValueError, "Unable to identify"):
            processor._normalize_report_total_column(df)


if __name__ == "__main__":
    unittest.main()
