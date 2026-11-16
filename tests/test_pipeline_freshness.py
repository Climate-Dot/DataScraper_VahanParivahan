"""Tests for the on-VM pipeline freshness check.

The load-recency half of this module exists because of a specific trap, and
`test_max_date_equal_to_previous_month_does_not_mask_a_missed_run` is the test
that holds the door shut on it. The month-settled half exists because a run can
land and still capture an in-flight month, and
`test_stub_month_with_healthy_row_count_is_not_settled` is the test that pins
`SUM([total])` as the metric rather than `COUNT(*)`.
"""

import importlib
import os
import sys
import unittest
from datetime import date, datetime, timedelta
from unittest import mock

repo_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if repo_path not in sys.path:
    sys.path.append(repo_path)
ops_path = os.path.join(repo_path, "ops")
if ops_path not in sys.path:
    sys.path.append(ops_path)

import check_pipeline_freshness as freshness

TABLE = "fact_ev_data_by_rto_v2"
SCOPE = "ALL_STATUSES"
PIPELINE = "new_portal_rto_freshness"

# The real September 2026 stub against the real August 2026 month. Row ratio
# 0.9255 looks healthy; total ratio 0.7763 does not.
SEPTEMBER_STUB = freshness.MonthStats(row_count=15049, total_registrations=1904174)
AUGUST_SETTLED = freshness.MonthStats(row_count=16261, total_registrations=2452785)


class FakeCursor:
    def __init__(self, results):
        self.results = list(results)
        self.executed = []
        self._current = []

    def execute(self, sql, *params):
        self.executed.append((" ".join(sql.split()), params))
        self._current = self.results.pop(0) if self.results else []
        return self

    def fetchone(self):
        return self._current[0] if self._current else None

    def fetchall(self):
        return list(self._current)

    def close(self):
        pass


class FakeConnection:
    def __init__(self, results=()):
        self.fake_cursor = FakeCursor(results)
        self.closed = False

    def cursor(self):
        return self.fake_cursor

    def close(self):
        self.closed = True


class LoadRecencyTests(unittest.TestCase):
    def setUp(self):
        self.reference = datetime(2026, 11, 16, 6, 15)

    def test_recent_load_is_not_stale(self):
        verdict = freshness.evaluate_load_recency(
            self.reference - timedelta(days=4), self.reference, max_load_age_days=35
        )
        self.assertFalse(verdict.is_stale)
        self.assertAlmostEqual(verdict.age_days, 4.0, places=3)

    def test_load_older_than_limit_is_stale(self):
        verdict = freshness.evaluate_load_recency(
            self.reference - timedelta(days=40), self.reference, max_load_age_days=35
        )
        self.assertTrue(verdict.is_stale)
        self.assertIn("40", verdict.message)

    def test_never_loaded_is_stale(self):
        verdict = freshness.evaluate_load_recency(
            None, self.reference, max_load_age_days=35
        )
        self.assertTrue(verdict.is_stale)
        self.assertIsNone(verdict.age_days)

    def test_thirty_three_day_gap_is_not_stale(self):
        # The legitimate worst case: last success on day 3 of a 31-day month,
        # next success on day 5 of the following month.
        verdict = freshness.evaluate_load_recency(
            self.reference - timedelta(days=33), self.reference, max_load_age_days=35
        )
        self.assertFalse(verdict.is_stale)

    def test_max_date_equal_to_previous_month_does_not_mask_a_missed_run(self):
        # Every V2 run writes rows for the in-flight month, so MAX([date]) is
        # always the current month and is useless as a freshness signal. A table
        # whose newest [date] is December but whose newest load is 40 days old
        # has a dead pipeline, and must be reported stale.
        verdict = freshness.evaluate_load_recency(
            datetime(2026, 12, 3, 4, 0),
            datetime(2027, 1, 12, 6, 15),
            max_load_age_days=35,
        )
        self.assertTrue(verdict.is_stale)
        # And the module must never key the staleness signal on [date] at all.
        self.assertNotIn("MAX([date])", freshness.LAST_LOADED_AT_QUERY)
        self.assertIn("MAX([inserted_at])", freshness.LAST_LOADED_AT_QUERY)


class TargetMonthTests(unittest.TestCase):
    def test_target_month_is_previous_calendar_month(self):
        self.assertEqual(freshness.target_month(date(2026, 11, 16)), (2026, 10))

    def test_target_month_crosses_the_year_boundary(self):
        self.assertEqual(freshness.target_month(date(2026, 1, 3)), (2025, 12))

    def test_preceding_month_crosses_the_year_boundary(self):
        self.assertEqual(freshness.preceding_month(2026, 1), (2025, 12))


class MonthSettledTests(unittest.TestCase):
    def test_month_with_full_total_is_settled(self):
        target = freshness.MonthStats(row_count=16200, total_registrations=2400000)
        verdict = freshness.evaluate_month_settled(
            target, AUGUST_SETTLED, min_month_total_ratio=0.85
        )
        self.assertFalse(verdict.is_stale)

    def test_stub_month_with_healthy_row_count_is_not_settled(self):
        # This is the test that pins the metric choice. Switch the comparison
        # back to COUNT(*) and only this one fails.
        verdict = freshness.evaluate_month_settled(
            SEPTEMBER_STUB, AUGUST_SETTLED, min_month_total_ratio=0.85
        )
        self.assertTrue(verdict.is_stale)

    def test_row_count_ratio_alone_would_have_passed_the_stub(self):
        row_ratio = SEPTEMBER_STUB.row_count / AUGUST_SETTLED.row_count
        total_ratio = (
            SEPTEMBER_STUB.total_registrations / AUGUST_SETTLED.total_registrations
        )
        self.assertGreater(row_ratio, 0.85)
        self.assertLess(total_ratio, 0.85)

    def test_verdict_message_reports_row_ratio_as_context(self):
        verdict = freshness.evaluate_month_settled(
            SEPTEMBER_STUB, AUGUST_SETTLED, min_month_total_ratio=0.85
        )
        self.assertIn("0.7763", verdict.message)
        self.assertIn("0.9255", verdict.message)
        self.assertIn("context", verdict.message.lower())

    def test_three_day_stub_month_is_not_settled(self):
        verdict = freshness.evaluate_month_settled(
            freshness.MonthStats(row_count=6662, total_registrations=170608),
            AUGUST_SETTLED,
            min_month_total_ratio=0.85,
        )
        self.assertTrue(verdict.is_stale)

    def test_missing_target_month_is_not_settled(self):
        verdict = freshness.evaluate_month_settled(
            None, AUGUST_SETTLED, min_month_total_ratio=0.85
        )
        self.assertTrue(verdict.is_stale)
        self.assertIn("no rows", verdict.message.lower())

    def test_missing_preceding_month_does_not_alert(self):
        # No baseline means no comparison. Firing here would train the operator
        # to ignore the channel, which is worse than not firing.
        verdict = freshness.evaluate_month_settled(
            AUGUST_SETTLED, None, min_month_total_ratio=0.85
        )
        self.assertFalse(verdict.is_stale)
        self.assertIn("no preceding", verdict.message.lower())


class QueryTests(unittest.TestCase):
    def test_fetch_last_loaded_at_reads_the_scalar(self):
        loaded = datetime(2026, 11, 3, 4, 0)
        connection = FakeConnection([[(loaded,)]])
        self.assertEqual(
            freshness.fetch_last_loaded_at(connection, TABLE, status_scope=SCOPE),
            loaded,
        )
        sql, params = connection.fake_cursor.executed[0]
        self.assertIn("MAX([inserted_at])", sql)
        self.assertEqual(params[0], (SCOPE,))

    def test_fetch_last_loaded_at_handles_an_empty_table(self):
        connection = FakeConnection([[(None,)]])
        self.assertIsNone(
            freshness.fetch_last_loaded_at(connection, TABLE, status_scope=SCOPE)
        )

    def test_fetch_month_row_counts_keys_by_month(self):
        connection = FakeConnection(
            [[(2026, 9, 15049, 1904174), (2026, 10, 16546, 3172712)]]
        )
        stats = freshness.fetch_month_row_counts(
            connection, TABLE, status_scope=SCOPE, year=2026, months=(9, 10)
        )
        self.assertEqual(stats[9].total_registrations, 1904174)
        self.assertEqual(stats[10].row_count, 16546)

    def test_month_stats_pair_queries_each_year_across_the_boundary(self):
        # Target (2026, 1) and preceding (2025, 12) are in different years, so a
        # single `WHERE [year] = ?` query cannot fetch both.
        connection = FakeConnection(
            [[(2026, 1, 15877, 2845531)], [(2025, 12, 15752, 2146602)]]
        )
        target, preceding = freshness.fetch_month_stats_pair(
            connection, TABLE, status_scope=SCOPE, target=(2026, 1), preceding=(2025, 12)
        )
        self.assertEqual(len(connection.fake_cursor.executed), 2)
        self.assertEqual(target.total_registrations, 2845531)
        self.assertEqual(preceding.total_registrations, 2146602)

    def test_month_stats_pair_uses_one_query_within_a_year(self):
        connection = FakeConnection(
            [[(2026, 9, 15049, 1904174), (2026, 10, 16546, 3172712)]]
        )
        target, preceding = freshness.fetch_month_stats_pair(
            connection, TABLE, status_scope=SCOPE, target=(2026, 10), preceding=(2026, 9)
        )
        self.assertEqual(len(connection.fake_cursor.executed), 1)
        self.assertEqual(target.row_count, 16546)
        self.assertEqual(preceding.row_count, 15049)


class MainTests(unittest.TestCase):
    def argv(self):
        return [
            "--table", TABLE,
            "--status-scope", SCOPE,
            "--max-load-age-days", "35",
            "--min-month-total-ratio", "0.85",
            "--pipeline", PIPELINE,
        ]

    def run_main(self, *, loaded_at, target, preceding, reference=datetime(2026, 11, 16, 6, 15)):
        connection = FakeConnection()
        with mock.patch.object(freshness, "connect", return_value=connection), \
             mock.patch.object(freshness, "_now", return_value=reference), \
             mock.patch.object(
                 freshness, "fetch_last_loaded_at", return_value=loaded_at
             ), \
             mock.patch.object(
                 freshness,
                 "fetch_month_stats_pair",
                 return_value=(target, preceding),
             ), \
             mock.patch("subprocess.run") as run:
            exit_code = freshness.main(self.argv())
        return exit_code, run

    def test_main_sends_no_alert_when_both_checks_pass(self):
        exit_code, run = self.run_main(
            loaded_at=datetime(2026, 11, 3, 4, 0),
            target=freshness.MonthStats(16546, 3172712),
            preceding=AUGUST_SETTLED,
        )
        self.assertEqual(exit_code, 0)
        run.assert_not_called()

    def test_main_sends_one_alert_naming_which_check_failed(self):
        exit_code, run = self.run_main(
            loaded_at=datetime(2026, 9, 24, 17, 20),
            target=SEPTEMBER_STUB,
            preceding=AUGUST_SETTLED,
        )
        self.assertEqual(exit_code, 1)
        self.assertEqual(run.call_count, 1)
        argv = run.call_args[0][0]
        self.assertIn("--status", argv)
        self.assertIn("FAILED", argv)
        self.assertIn("--pipeline", argv)
        self.assertIn(PIPELINE, argv)
        joined = " ".join(str(part) for part in argv).lower()
        self.assertIn("load recency", joined)
        self.assertIn("month settled", joined)

    def test_main_alert_names_only_the_failing_check(self):
        exit_code, run = self.run_main(
            loaded_at=datetime(2026, 11, 3, 4, 0),
            target=SEPTEMBER_STUB,
            preceding=AUGUST_SETTLED,
        )
        self.assertEqual(exit_code, 1)
        joined = " ".join(str(part) for part in run.call_args[0][0]).lower()
        self.assertIn("month settled", joined)
        self.assertNotIn("load recency is stale", joined)


class ImportSafetyTests(unittest.TestCase):
    def test_module_imports_without_pyodbc(self):
        with mock.patch.dict(sys.modules, {"pyodbc": None}):
            reloaded = importlib.reload(freshness)
            self.assertIsNone(reloaded.pyodbc)
        importlib.reload(freshness)

    def test_connect_without_pyodbc_raises_a_clear_error(self):
        with mock.patch.object(freshness, "pyodbc", None):
            with self.assertRaises(RuntimeError) as caught:
                freshness.connect()
        self.assertIn("pyodbc", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
