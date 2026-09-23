import datetime as dt
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from new_portal import rto_blob_snapshot as snapshot


class ResourceExists(Exception):
    """Stands in for azure.core.exceptions.ResourceExistsError, absent in CI."""


class BlobNameTests(unittest.TestCase):
    def test_name_carries_year_and_fetch_time(self):
        when = dt.datetime(2026, 8, 20, 5, 17, 1, tzinfo=dt.timezone.utc)
        self.assertEqual(
            snapshot.build_snapshot_blob_name(2026, when),
            "year=2026/new_portal_rto_ev_data_2026__20260820T051701Z.csv",
        )

    def test_name_is_normalised_to_utc(self):
        # Otherwise two runs an hour apart in different offsets could collide
        # or sort wrongly against each other.
        ist = dt.timezone(dt.timedelta(hours=5, minutes=30))
        local = dt.datetime(2026, 8, 20, 10, 47, 1, tzinfo=ist)
        utc = dt.datetime(2026, 8, 20, 5, 17, 1, tzinfo=dt.timezone.utc)
        self.assertEqual(
            snapshot.build_snapshot_blob_name(2026, local),
            snapshot.build_snapshot_blob_name(2026, utc),
        )

    def test_two_runs_in_the_same_month_get_distinct_names(self):
        # The whole point: several runs per month must not collapse into one blob.
        a = dt.datetime(2026, 8, 20, 5, 0, 0, tzinfo=dt.timezone.utc)
        b = dt.datetime(2026, 8, 27, 5, 0, 0, tzinfo=dt.timezone.utc)
        self.assertNotEqual(
            snapshot.build_snapshot_blob_name(2026, a),
            snapshot.build_snapshot_blob_name(2026, b),
        )

    def test_year_prefix_is_hive_style(self):
        when = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
        self.assertTrue(snapshot.build_snapshot_blob_name(2026, when).startswith("year=2026/"))


class ContainerNameTests(unittest.TestCase):
    def test_config_key_wins_when_present(self):
        config = {"storage": {snapshot.CONTAINER_CONFIG_KEY: "custom-container"}}
        self.assertEqual(snapshot.resolve_container_name(config), "custom-container")

    def test_falls_back_to_default_without_the_key(self):
        # config.yaml lives only on the VM; requiring a new key would break a
        # fresh checkout until someone hand-edits it.
        self.assertEqual(
            snapshot.resolve_container_name({"storage": {}}),
            snapshot.DEFAULT_CONTAINER_NAME,
        )

    def test_falls_back_without_a_storage_section_at_all(self):
        self.assertEqual(snapshot.resolve_container_name({}), snapshot.DEFAULT_CONTAINER_NAME)


class UploadSnapshotTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.csv = Path(self.tmp.name) / "new_portal_rto_ev_data_2026.csv"
        self.csv.write_text("year,month\n2026,8\n", encoding="utf-8")
        self.when = dt.datetime(2026, 8, 20, 5, 17, 1, tzinfo=dt.timezone.utc)

    def _container(self):
        container = mock.Mock()
        container.get_blob_client.return_value = mock.Mock()
        return container

    def test_uploads_without_overwriting(self):
        # Overwriting would destroy exactly the history these snapshots exist for.
        container = self._container()

        name, uploaded = snapshot.upload_snapshot(
            self.csv, 2026, container_client=container, fetched_at=self.when
        )

        self.assertTrue(uploaded)
        self.assertIn("20260820T051701Z", name)
        _, kwargs = container.get_blob_client.return_value.upload_blob.call_args
        self.assertIs(kwargs["overwrite"], False)

    def test_existing_snapshot_is_left_alone_and_not_an_error(self):
        container = self._container()
        with mock.patch.object(snapshot, "ResourceExistsError", ResourceExists):
            container.get_blob_client.return_value.upload_blob.side_effect = ResourceExists()
            name, uploaded = snapshot.upload_snapshot(
                self.csv, 2026, container_client=container, fetched_at=self.when
            )

        self.assertFalse(uploaded)
        self.assertIn("20260820T051701Z", name)

    def test_missing_csv_raises_with_a_useful_hint(self):
        with self.assertRaises(FileNotFoundError) as caught:
            snapshot.upload_snapshot(
                Path(self.tmp.name) / "nope.csv", 2026, container_client=self._container()
            )
        self.assertIn("rto_fetch", str(caught.exception))

    def test_timestamp_defaults_to_the_files_own_mtime(self):
        # Makes re-uploading the same artifact idempotent rather than creating a
        # second near-identical snapshot.
        container = self._container()
        name, _ = snapshot.upload_snapshot(self.csv, 2026, container_client=container)
        expected = snapshot.build_snapshot_blob_name(2026, snapshot.snapshot_timestamp(self.csv))
        self.assertEqual(name, expected)


class ImportSafetyTests(unittest.TestCase):
    def test_module_imports_without_azure(self):
        # CLAUDE.md's CI rule: anything a test imports must load without azure
        # or pyodbc installed.
        self.assertIsNotNone(snapshot.build_snapshot_blob_name)


if __name__ == "__main__":
    unittest.main()
