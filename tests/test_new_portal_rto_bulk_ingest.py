import re
import unittest
from pathlib import Path
from unittest import mock

from new_portal import rto_bulk_ingest as bulk
from new_portal.schema import CSV_COLUMNS

MIGRATIONS = Path(__file__).resolve().parent.parent / "sql" / "migrations"
LANDING_MIGRATION = MIGRATIONS / "2026-09-29_new_portal_bulk_landing_table.sql"
V2_MIGRATION = MIGRATIONS / "2026-08-18_new_portal_rto_v2_tables.sql"


def _columns_of(sql: str, table: str) -> list[str]:
    block = re.search(rf"CREATE TABLE dbo\.{table} \((.*?)\n\s*\);", sql, re.S)
    assert block, f"{table} not found"
    return re.findall(r"^\s*\[(\w+)\]", block.group(1), re.M)


class LandingSchemaTests(unittest.TestCase):
    """The landing table must mirror the CSV exactly.

    BULK INSERT maps columns by position, so a drift between the file's column
    order and the table's silently writes values into the wrong columns rather
    than failing.
    """

    def test_landing_matches_csv_columns_in_order(self):
        cols = _columns_of(LANDING_MIGRATION.read_text(), "landing_fact_ev_data_by_rto_v2")
        self.assertEqual(cols, CSV_COLUMNS)

    def test_landing_is_staging_minus_inserted_at(self):
        landing = _columns_of(LANDING_MIGRATION.read_text(), "landing_fact_ev_data_by_rto_v2")
        staging = _columns_of(V2_MIGRATION.read_text(), "staging_fact_ev_data_by_rto_v2")
        self.assertEqual(landing, [c for c in staging if c != "inserted_at"])

    def test_landing_column_types_match_staging(self):
        def typed(sql, table):
            block = re.search(rf"CREATE TABLE dbo\.{table} \((.*?)\n\s*\);", sql, re.S).group(1)
            out = {}
            for line in block.strip().splitlines():
                m = re.match(r"\s*\[(\w+)\]\s+([A-Z0-9()a-z, ]+?)(\s+NULL|\s+NOT NULL)", line)
                if m:
                    out[m.group(1)] = m.group(2).strip()
            return out

        landing = typed(LANDING_MIGRATION.read_text(), "landing_fact_ev_data_by_rto_v2")
        staging = typed(V2_MIGRATION.read_text(), "staging_fact_ev_data_by_rto_v2")
        for col, kind in landing.items():
            self.assertEqual(kind, staging[col], f"{col} type drifted from staging")

    def test_migration_is_idempotent(self):
        sql = LANDING_MIGRATION.read_text()
        self.assertIn("IF OBJECT_ID('dbo.landing_fact_ev_data_by_rto_v2') IS NOT NULL", sql)


class BulkInsertStatementTests(unittest.TestCase):
    """Format options are pinned because getting them wrong mangles data silently."""

    def test_statement_pins_every_format_option(self):
        sql = bulk.build_bulk_insert("year=2025/file.csv")
        self.assertIn("FORMAT='CSV'", sql)
        self.assertIn("FIRSTROW=2", sql)            # skip the header
        self.assertIn("ROWTERMINATOR='0x0d0a'", sql)  # csv.writer emits CRLF
        self.assertIn("CODEPAGE='65001'", sql)      # the CSV is UTF-8
        self.assertIn('FIELDQUOTE=\'"\'', sql)      # classes contain commas
        self.assertIn(f"DATA_SOURCE='{bulk.DATA_SOURCE_NAME}'", sql)
        self.assertIn("year=2025/file.csv", sql)

    def test_it_targets_landing_not_staging_or_final(self):
        sql = bulk.build_bulk_insert("year=2025/file.csv")
        self.assertIn(bulk.LANDING_TABLE, sql)
        self.assertNotIn("staging_fact", sql)
        self.assertNotIn("INSERT INTO fact_ev_data_by_rto_v2", sql)


class DeleteScopeTests(unittest.TestCase):
    def test_delete_scope_matches_the_row_by_row_ingest(self):
        """Both paths must replace the same slice, or they diverge by route."""
        from new_portal.rto_ingest import REPLACEMENT_SCOPE_COLUMNS

        sql = bulk.build_delete_query()
        for col in REPLACEMENT_SCOPE_COLUMNS:
            self.assertIn(f"s.{col} = fact_ev_data_by_rto_v2.{col}", sql)
        self.assertIn("DELETE FROM fact_ev_data_by_rto_v2", sql)

    def test_delete_is_scoped_not_wholesale(self):
        self.assertIn("WHERE EXISTS", bulk.build_delete_query())


class _Blob:
    """Mock(name=...) is reserved by the Mock constructor, so use a plain stub."""

    def __init__(self, name):
        self.name = name


class SnapshotSelectionTests(unittest.TestCase):
    def test_picks_the_newest_snapshot_for_the_year(self):
        container = mock.Mock()
        container.list_blobs.return_value = [
            _Blob("year=2025/new_portal_rto_ev_data_2025__20260101T000000Z.csv"),
            _Blob("year=2025/new_portal_rto_ev_data_2025__20260925T040128Z.csv"),
            _Blob("year=2025/new_portal_rto_ev_data_2025__20260501T000000Z.csv"),
        ]
        self.assertEqual(
            bulk.latest_snapshot_blob(container, 2025),
            "year=2025/new_portal_rto_ev_data_2025__20260925T040128Z.csv",
        )

    def test_missing_snapshot_says_what_to_run(self):
        container = mock.Mock()
        container.list_blobs.return_value = []
        with self.assertRaises(FileNotFoundError) as caught:
            bulk.latest_snapshot_blob(container, 2025)
        self.assertIn("rto_blob_snapshot", str(caught.exception))

    def test_non_csv_blobs_are_ignored(self):
        container = mock.Mock()
        container.list_blobs.return_value = [
            _Blob("year=2025/new_portal_rto_ev_data_2025__20260101T000000Z.csv"),
            _Blob("year=2025/new_portal_rto_ev_data_2025__20260601T000000Z.csv.bak"),
        ]
        self.assertTrue(bulk.latest_snapshot_blob(container, 2025).endswith(".csv"))


class CredentialHygieneTests(unittest.TestCase):
    def test_sas_is_short_lived(self):
        # A credential stored in the database should expire quickly.
        self.assertLessEqual(bulk.SAS_LIFETIME_HOURS, 6)

    def test_credential_is_recreated_not_reused(self):
        cursor = mock.Mock()
        bulk.refresh_external_data_source(cursor, "acct", "cont", "sastoken")
        executed = " ".join(c.args[0] for c in cursor.execute.call_args_list)
        self.assertIn("DROP EXTERNAL DATA SOURCE", executed)
        self.assertIn("DROP DATABASE SCOPED CREDENTIAL", executed)
        self.assertIn("CREATE DATABASE SCOPED CREDENTIAL", executed)
        self.assertIn("https://acct.blob.core.windows.net/cont", executed)


class ImportSafetyTests(unittest.TestCase):
    def test_module_imports_without_azure_or_pyodbc(self):
        # CLAUDE.md's CI rule.
        self.assertIsNotNone(bulk.build_bulk_insert)


if __name__ == "__main__":
    unittest.main()
