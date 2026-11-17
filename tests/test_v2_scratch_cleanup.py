"""Tests for the V2 scratch-table cleanup.

`test_never_touches_the_fact_table` is the test that matters here; everything
else is bookkeeping. This script exists to remove a manual-recovery foot-gun,
and a bug in it would be the very thing it is meant to prevent.
"""

import importlib
import os
import sys
import unittest
from unittest import mock

repo_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if repo_path not in sys.path:
    sys.path.append(repo_path)
ops_path = os.path.join(repo_path, "ops")
if ops_path not in sys.path:
    sys.path.append(ops_path)

import truncate_v2_scratch_tables as cleanup

FACT_TABLE = "fact_ev_data_by_rto_v2"


class RecordingCursor:
    def __init__(self, counts):
        self.counts = dict(counts)
        self.statements = []
        self._last = ""

    def execute(self, sql, *params):
        self._last = sql
        self.statements.append(" ".join(sql.split()))
        return self

    def fetchone(self):
        for table, count in self.counts.items():
            if table in self._last:
                return (count,)
        return (0,)

    def close(self):
        pass


class RecordingConnection:
    def __init__(self, counts=None):
        self.recording_cursor = RecordingCursor(counts or {})
        self.commits = 0

    def cursor(self):
        return self.recording_cursor

    def commit(self):
        self.commits += 1

    def close(self):
        pass


def default_counts():
    return {table: 160573 for table in cleanup.SCRATCH_TABLES}


class ScratchTableIdentityTests(unittest.TestCase):
    def test_scratch_tables_are_the_landing_and_staging_tables(self):
        from new_portal.rto_bulk_ingest import LANDING_TABLE
        from new_portal.rto_ingest import STAGING_TABLE

        self.assertEqual(cleanup.SCRATCH_TABLES, (LANDING_TABLE, STAGING_TABLE))

    def test_the_fact_table_is_not_a_scratch_table(self):
        from new_portal.rto_ingest import FINAL_TABLE

        self.assertNotIn(FINAL_TABLE, cleanup.SCRATCH_TABLES)


class TruncateTests(unittest.TestCase):
    def test_truncate_issues_one_truncate_per_table(self):
        connection = RecordingConnection(default_counts())
        cleanup.truncate_scratch_tables(connection)
        truncates = [
            statement
            for statement in connection.recording_cursor.statements
            if statement.upper().startswith("TRUNCATE")
        ]
        self.assertEqual(len(truncates), len(cleanup.SCRATCH_TABLES))

    def test_truncate_counts_rows_before_removing_them(self):
        connection = RecordingConnection(default_counts())
        removed = cleanup.truncate_scratch_tables(connection)
        statements = connection.recording_cursor.statements
        for table in cleanup.SCRATCH_TABLES:
            count_index = next(
                index
                for index, statement in enumerate(statements)
                if "COUNT(*)" in statement.upper() and table in statement
            )
            truncate_index = next(
                index
                for index, statement in enumerate(statements)
                if statement.upper().startswith("TRUNCATE") and table in statement
            )
            self.assertLess(count_index, truncate_index)
            self.assertEqual(removed[table], 160573)

    def test_dry_run_issues_no_truncate(self):
        connection = RecordingConnection(default_counts())
        removed = cleanup.truncate_scratch_tables(connection, dry_run=True)
        self.assertFalse(
            any(
                statement.upper().startswith("TRUNCATE")
                for statement in connection.recording_cursor.statements
            )
        )
        # It still reports what it would have cleared.
        self.assertEqual(set(removed), set(cleanup.SCRATCH_TABLES))
        self.assertEqual(connection.commits, 0)

    def test_already_empty_table_is_success_not_an_error(self):
        connection = RecordingConnection({table: 0 for table in cleanup.SCRATCH_TABLES})
        removed = cleanup.truncate_scratch_tables(connection)
        self.assertEqual(set(removed.values()), {0})

    def test_never_touches_the_fact_table(self):
        connection = RecordingConnection(default_counts())
        cleanup.truncate_scratch_tables(connection)
        for statement in connection.recording_cursor.statements:
            for occurrence in _occurrences(statement, FACT_TABLE):
                prefix = statement[:occurrence]
                self.assertTrue(
                    prefix.endswith("landing_") or prefix.endswith("staging_"),
                    msg=f"statement touches the fact table directly: {statement}",
                )

    def test_a_table_outside_the_scratch_list_is_refused(self):
        connection = RecordingConnection({FACT_TABLE: 2341077})
        with self.assertRaises(ValueError) as caught:
            cleanup.truncate_scratch_tables(connection, tables=(FACT_TABLE,))
        self.assertIn(FACT_TABLE, str(caught.exception))
        self.assertEqual(connection.recording_cursor.statements, [])

    def test_commit_happens_once_after_a_real_run(self):
        connection = RecordingConnection(default_counts())
        cleanup.truncate_scratch_tables(connection)
        self.assertEqual(connection.commits, 1)


class MainTests(unittest.TestCase):
    def test_dry_run_main_returns_zero_and_truncates_nothing(self):
        connection = RecordingConnection(default_counts())
        with mock.patch.object(cleanup, "connect", return_value=connection):
            self.assertEqual(cleanup.main(["--dry-run"]), 0)
        self.assertFalse(
            any(
                statement.upper().startswith("TRUNCATE")
                for statement in connection.recording_cursor.statements
            )
        )

    def test_main_returns_zero_on_a_real_run(self):
        connection = RecordingConnection(default_counts())
        with mock.patch.object(cleanup, "connect", return_value=connection):
            self.assertEqual(cleanup.main([]), 0)
        self.assertEqual(connection.commits, 1)


class ImportSafetyTests(unittest.TestCase):
    def test_module_imports_without_pyodbc(self):
        with mock.patch.dict(sys.modules, {"pyodbc": None}):
            reloaded = importlib.reload(cleanup)
            self.assertIsNone(reloaded.pyodbc)
        importlib.reload(cleanup)

    def test_connect_without_pyodbc_raises_a_clear_error(self):
        with mock.patch.object(cleanup, "pyodbc", None):
            with self.assertRaises(RuntimeError) as caught:
                cleanup.connect()
        self.assertIn("pyodbc", str(caught.exception))


def _occurrences(haystack, needle):
    start = haystack.find(needle)
    while start != -1:
        yield start
        start = haystack.find(needle, start + 1)


if __name__ == "__main__":
    unittest.main()
