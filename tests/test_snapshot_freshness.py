"""Tests for the off-VM snapshot watchdog.

This is the only check that still works when the VM never powered on, so the
tests care as much about it degrading loudly (a missing container must alert,
not crash) as about the arithmetic.
"""

import importlib
import os
import sys
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

repo_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if repo_path not in sys.path:
    sys.path.append(repo_path)
ops_path = os.path.join(repo_path, "ops")
if ops_path not in sys.path:
    sys.path.append(ops_path)

import check_snapshot_freshness as watchdog

NOW = datetime(2026, 11, 16, 6, 0, tzinfo=timezone.utc)


class FakeBlob:
    def __init__(self, name, last_modified):
        self.name = name
        self.last_modified = last_modified


class FakeContainerClient:
    def __init__(self, blobs=(), error=None):
        self.blobs = list(blobs)
        self.error = error

    def list_blobs(self):
        if self.error is not None:
            raise self.error
        return iter(self.blobs)


class AgeTests(unittest.TestCase):
    def test_newest_snapshot_age_uses_the_newest_blob(self):
        blobs = [
            ("year=2026/a.csv", NOW - timedelta(days=40)),
            ("year=2026/b.csv", NOW - timedelta(days=12)),
            ("year=2026/c.csv", NOW - timedelta(days=30)),
        ]
        self.assertAlmostEqual(
            watchdog.newest_snapshot_age_days(blobs, NOW), 12.0, places=3
        )

    def test_no_blobs_returns_none(self):
        self.assertIsNone(watchdog.newest_snapshot_age_days([], NOW))

    def test_naive_timestamps_are_treated_as_utc(self):
        blobs = [("year=2026/a.csv", datetime(2026, 11, 4, 6, 0))]
        self.assertAlmostEqual(
            watchdog.newest_snapshot_age_days(blobs, NOW), 12.0, places=3
        )


class VerdictTests(unittest.TestCase):
    def test_age_within_limit_is_not_stale(self):
        verdict = watchdog.evaluate_snapshot_freshness(12.0, max_age_days=40)
        self.assertFalse(verdict.is_stale)

    def test_age_beyond_limit_is_stale(self):
        verdict = watchdog.evaluate_snapshot_freshness(45.0, max_age_days=40)
        self.assertTrue(verdict.is_stale)
        self.assertIn("45", verdict.message)

    def test_no_snapshot_at_all_is_stale(self):
        verdict = watchdog.evaluate_snapshot_freshness(None, max_age_days=40)
        self.assertTrue(verdict.is_stale)

    def test_stale_message_mentions_the_vm_may_not_have_started(self):
        for age in (None, 45.0):
            verdict = watchdog.evaluate_snapshot_freshness(age, max_age_days=40)
            self.assertIn("vm", verdict.message.lower())
            self.assertIn("never started", verdict.message.lower())


class ListSnapshotTests(unittest.TestCase):
    def test_lists_name_and_timestamp_pairs(self):
        client = FakeContainerClient(
            [FakeBlob("year=2026/a.csv", NOW - timedelta(days=3))]
        )
        listed = watchdog.list_snapshot_times(client)
        self.assertEqual(listed[0][0], "year=2026/a.csv")

    def test_missing_container_is_stale_not_an_error(self):
        client = FakeContainerClient(error=watchdog.ResourceNotFoundError("gone"))
        self.assertIsNone(watchdog.list_snapshot_times(client))
        verdict = watchdog.evaluate_snapshot_freshness(None, max_age_days=40)
        self.assertTrue(verdict.is_stale)

    def test_empty_container_is_an_empty_list_not_none(self):
        # Absent and empty must stay distinguishable: both alert, but the
        # message differs and only one of them means "misconfigured".
        self.assertEqual(watchdog.list_snapshot_times(FakeContainerClient([])), [])


class ContainerNameTests(unittest.TestCase):
    def test_container_name_is_imported_not_respelled(self):
        from new_portal.rto_blob_snapshot import DEFAULT_CONTAINER_NAME

        self.assertIs(watchdog.CONTAINER_NAME, DEFAULT_CONTAINER_NAME)


class MainTests(unittest.TestCase):
    def run_main(self, client, extra=()):
        with mock.patch.object(
            watchdog, "build_container_client", return_value=client
        ), mock.patch.object(watchdog, "_now", return_value=NOW), mock.patch(
            "subprocess.run"
        ) as run:
            exit_code = watchdog.main(["--max-age-days", "40", *extra])
        return exit_code, run

    def test_fresh_snapshot_exits_zero_and_sends_nothing(self):
        client = FakeContainerClient(
            [FakeBlob("year=2026/a.csv", NOW - timedelta(days=12))]
        )
        exit_code, run = self.run_main(client)
        self.assertEqual(exit_code, 0)
        run.assert_not_called()

    def test_stale_snapshot_exits_one_and_alerts(self):
        client = FakeContainerClient(
            [FakeBlob("year=2026/a.csv", NOW - timedelta(days=45))]
        )
        exit_code, run = self.run_main(client)
        self.assertEqual(exit_code, 1)
        self.assertEqual(run.call_count, 1)
        argv = run.call_args[0][0]
        self.assertIn("FAILED", argv)
        joined = " ".join(str(part) for part in argv).lower()
        self.assertIn("never started", joined)

    def test_missing_container_alerts_without_raising(self):
        client = FakeContainerClient(error=watchdog.ResourceNotFoundError("gone"))
        exit_code, run = self.run_main(client)
        self.assertEqual(exit_code, 1)
        self.assertEqual(run.call_count, 1)


class ImportSafetyTests(unittest.TestCase):
    def test_module_imports_without_azure(self):
        with mock.patch.dict(
            sys.modules,
            {"azure": None, "azure.storage": None, "azure.storage.blob": None},
        ):
            reloaded = importlib.reload(watchdog)
            self.assertIsNone(reloaded.BlobServiceClient)
        importlib.reload(watchdog)

    def test_build_container_client_without_azure_raises_a_clear_error(self):
        with mock.patch.object(watchdog, "BlobServiceClient", None):
            with self.assertRaises(RuntimeError) as caught:
                watchdog.build_container_client(connection_string="x")
        self.assertIn("azure-storage-blob", str(caught.exception))

    def test_missing_connection_string_raises_a_clear_error(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(RuntimeError) as caught:
                watchdog.build_container_client()
        self.assertIn(watchdog.CONNECTION_STRING_ENV_VAR, str(caught.exception))


if __name__ == "__main__":
    unittest.main()
