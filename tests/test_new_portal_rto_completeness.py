"""Tests for the independent completeness gate.

Hermetic: tmpdir fixtures only, no network, no database. The gate's whole job is
to decide from artifacts on disk whether a year's fetch output is safe to ingest,
so the fixtures are the artifacts.
"""

import csv
import json
import os
import sys
import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

repo_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if repo_path not in sys.path:
    sys.path.append(repo_path)

from new_portal.rto_completeness import main, summarize_csv, verify_run_complete
from new_portal.rto_fetch import (
    build_gap_manifest_path,
    build_output_path,
    build_progress_path,
)
from new_portal.schema import CSV_COLUMNS, STATUS_SCOPE_ALL

YEAR = 2026
TARGET_MONTH = 10


def make_record(**overrides):
    record = {
        "year": YEAR,
        "month": TARGET_MONTH,
        "date": f"{YEAR}-{TARGET_MONTH:02d}-01",
        "state": "Maharashtra",
        "state_code": "MH",
        "rto_code": 1,
        "rto_name": "MUMBAI CENTRAL",
        "vehicle_class": "MOTOR CAR",
        "status_scope": STATUS_SCOPE_ALL,
        "total": 5,
    }
    record.update(overrides)
    return record


def label(state_code, rto_code, rto_name):
    """The checkpoint key format from rto_fetch.label_of / Gap.rto_label."""
    return f"{state_code}/{rto_code} {rto_name}"


class CompletenessTestCase(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.directory = Path(self._tmp.name)
        self.addCleanup(self._tmp.cleanup)
        self.csv_path = build_output_path(YEAR, self.directory)
        self.progress_path = build_progress_path(YEAR, self.directory)
        self.manifest_path = build_gap_manifest_path(YEAR, self.directory)
        self.labels = [
            label("MH", 1, "MUMBAI CENTRAL"),
            label("MH", 2, "PUNE"),
            label("KL", 13, "KOZHIKODE"),
        ]

    def write_csv(self, records):
        with self.csv_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS)
            writer.writeheader()
            for record in records:
                row = {column: "" for column in CSV_COLUMNS}
                row.update(record)
                writer.writerow(row)

    def write_progress(self, labels, gapped=()):
        with self.progress_path.open("w", encoding="utf-8") as handle:
            for entry_label in labels:
                gaps = []
                if entry_label in gapped:
                    gaps = [
                        {
                            "state_code": "MH",
                            "rto_code": 1,
                            "rto_name": "MUMBAI CENTRAL",
                            "status_scope": STATUS_SCOPE_ALL,
                            "stage": "monthly",
                        }
                    ]
                handle.write(
                    json.dumps(
                        {"rto_label": entry_label, "rows": [], "gaps": gaps}
                    )
                    + "\n"
                )

    def write_manifest(self, gap_count=4):
        payload = {"year": YEAR, "gap_count": gap_count, "gaps": []}
        self.manifest_path.write_text(json.dumps(payload), encoding="utf-8")

    def set_up_clean_run(self):
        self.write_progress(self.labels)
        self.write_csv([make_record(), make_record(rto_code=2)])

    def verify(self, **overrides):
        kwargs = {
            "csv_path": self.csv_path,
            "gap_manifest_path": self.manifest_path,
            "expected_office_count": len(self.labels),
        }
        kwargs.update(overrides)
        return verify_run_complete(YEAR, TARGET_MONTH, **kwargs)


class VerifyRunCompleteTests(CompletenessTestCase):
    def test_clean_run_has_no_failures(self):
        self.set_up_clean_run()
        report = self.verify()
        self.assertEqual(report.failures, [])
        self.assertEqual(report.expected_offices, 3)
        self.assertEqual(report.complete_offices, 3)
        self.assertIn(TARGET_MONTH, report.months_present)
        self.assertFalse(report.gap_manifest_present)

    def test_gap_manifest_present_is_a_failure(self):
        self.set_up_clean_run()
        self.write_manifest()
        report = self.verify()
        self.assertTrue(report.gap_manifest_present)
        self.assertIn(
            str(self.manifest_path), " ".join(report.failures)
        )

    def test_partial_state_checkpoint_is_a_failure(self):
        self.write_progress(self.labels[:1])
        self.write_csv([make_record()])
        report = self.verify()
        joined = " ".join(report.failures)
        self.assertIn("1", joined)
        self.assertIn("3", joined)
        self.assertEqual(report.complete_offices, 1)

    def test_gapped_office_in_checkpoint_is_not_complete(self):
        self.write_progress(self.labels, gapped={self.labels[2]})
        self.write_csv([make_record()])
        report = self.verify()
        self.assertEqual(report.complete_offices, 2)
        self.assertNotEqual(report.failures, [])

    def test_csv_older_than_min_fetched_at_is_a_failure(self):
        self.set_up_clean_run()
        stale = time.time() - 7200
        os.utime(self.csv_path, (stale, stale))
        report = self.verify(min_fetched_at=time.time() - 60)
        self.assertTrue(report.stale_csv)
        self.assertNotEqual(report.failures, [])

    def test_csv_newer_than_min_fetched_at_is_not_stale(self):
        self.set_up_clean_run()
        report = self.verify(min_fetched_at=time.time() - 3600)
        self.assertFalse(report.stale_csv)
        self.assertEqual(report.failures, [])

    def test_target_month_absent_from_csv_is_a_failure(self):
        self.write_progress(self.labels)
        self.write_csv([make_record(month=9, date=f"{YEAR}-09-01")])
        report = self.verify()
        self.assertNotIn(TARGET_MONTH, report.months_present)
        self.assertIn(str(TARGET_MONTH), " ".join(report.failures))

    def test_unexpected_status_scope_in_csv_is_a_failure(self):
        self.write_progress(self.labels)
        self.write_csv([make_record(), make_record(status_scope="ACTIVE")])
        report = self.verify()
        self.assertIn("ACTIVE", " ".join(report.failures))

    def test_office_with_no_rows_but_complete_checkpoint_is_not_a_failure(self):
        # The first 13 offices in the national list are Andaman islands with no
        # registrations at all. They are complete in the checkpoint and absent
        # from the CSV, and that is legitimate.
        self.write_progress(self.labels)
        self.write_csv([make_record()])
        report = self.verify()
        self.assertEqual(report.failures, [])
        self.assertEqual(report.csv_offices, 1)
        self.assertEqual(report.complete_offices, 3)

    def test_partial_in_flight_current_month_is_not_a_failure(self):
        # A run on day 3 of November pulls Jan-Nov; November is a 3-day stub by
        # design. The gate asserts on the target (closed) month only.
        self.write_progress(self.labels)
        records = [
            make_record(month=month, date=f"{YEAR}-{month:02d}-01", total=5000)
            for month in range(1, TARGET_MONTH + 1)
        ]
        records.append(make_record(month=11, date=f"{YEAR}-11-01", total=7))
        self.write_csv(records)
        report = self.verify()
        self.assertEqual(report.failures, [])
        self.assertIn(11, report.months_present)

    def test_absent_checkpoint_with_fresh_csv_and_no_manifest_is_clean(self):
        # rto_fetch DELETES the checkpoint when every office finished cleanly
        # ("Run complete with no gaps; removed ...__progress.jsonl"). So the
        # on-disk signature of a successful sweep is: no checkpoint, no gap
        # manifest, a CSV this run wrote. Counting complete offices from a file
        # that success removes would reject every good run.
        self.write_csv([make_record()])
        self.assertFalse(self.progress_path.exists())
        report = self.verify(min_fetched_at=time.time() - 3600)
        self.assertEqual(report.failures, [])
        self.assertFalse(report.checkpoint_present)

    def test_absent_checkpoint_without_a_fetch_floor_is_a_failure(self):
        # With no floor, an absent checkpoint is indistinguishable from "no
        # sweep ever ran here", so nothing proves provenance.
        self.write_csv([make_record()])
        report = self.verify()
        self.assertNotEqual(report.failures, [])
        self.assertIn("provenance", " ".join(report.failures).lower())

    def test_absent_checkpoint_with_a_gap_manifest_is_still_a_failure(self):
        # --allow-gaps lets rto_fetch reach the checkpoint-deleting path WITH
        # gaps, but it writes the manifest first, so the manifest is what
        # separates that case from a genuinely clean one.
        self.write_csv([make_record()])
        self.write_manifest()
        report = self.verify(min_fetched_at=time.time() - 3600)
        self.assertNotEqual(report.failures, [])

    def test_absent_checkpoint_with_a_stale_csv_is_a_failure(self):
        self.write_csv([make_record()])
        stale = time.time() - 7200
        os.utime(self.csv_path, (stale, stale))
        report = self.verify(min_fetched_at=time.time() - 60)
        self.assertTrue(report.stale_csv)
        self.assertNotEqual(report.failures, [])

    def test_clean_completion_still_asserts_breadth(self):
        # The absent-checkpoint branch is the path production always takes,
        # because a successful sweep deletes its own checkpoint. If it asserts
        # nothing, the gate performs no coverage check at all on the real path:
        # a one-office CSV would sail through.
        self.write_csv([make_record()])
        report = verify_run_complete(
            YEAR,
            TARGET_MONTH,
            csv_path=self.csv_path,
            gap_manifest_path=self.manifest_path,
            expected_office_count=1676,
            min_fetched_at=time.time() - 3600,
        )
        self.assertNotEqual(report.failures, [])
        joined = " ".join(report.failures)
        self.assertIn("1", joined)
        self.assertIn("1676", joined)

    def test_clean_completion_accepts_realistic_breadth(self):
        # The real 2026 pull holds 1,398 offices against 1,676 targets: offices
        # that reported nothing all year contribute no rows, which is
        # legitimate. That must pass.
        self.write_csv(
            [
                make_record(state_code=f"S{index // 40}", rto_code=index)
                for index in range(1398)
            ]
        )
        report = verify_run_complete(
            YEAR,
            TARGET_MONTH,
            csv_path=self.csv_path,
            gap_manifest_path=self.manifest_path,
            expected_office_count=1676,
            min_fetched_at=time.time() - 3600,
        )
        self.assertEqual(report.failures, [])

    def test_checkpoint_labels_outside_the_target_list_do_not_count(self):
        # After a crosswalk refresh, stale checkpoint labels could otherwise
        # push complete_offices to the expected count while real coverage is
        # short, suppressing the only coverage failure the gate produces.
        self.write_progress(
            [
                "ZZ/9999 AN OFFICE THAT IS NOT IN THE CROSSWALK",
                "ZZ/9998 NOR IS THIS ONE",
            ]
        )
        self.write_csv([make_record()])
        report = verify_run_complete(
            YEAR,
            TARGET_MONTH,
            csv_path=self.csv_path,
            gap_manifest_path=self.manifest_path,
        )
        self.assertEqual(report.complete_offices, 0)
        self.assertNotEqual(report.failures, [])

    def test_missing_csv_is_a_failure(self):
        self.write_progress(self.labels)
        report = self.verify()
        self.assertNotEqual(report.failures, [])

    def test_expected_office_count_defaults_to_the_live_crosswalk(self):
        self.write_progress(self.labels)
        self.write_csv([make_record()])
        report = verify_run_complete(
            YEAR,
            TARGET_MONTH,
            csv_path=self.csv_path,
            gap_manifest_path=self.manifest_path,
        )
        # The live crosswalk holds well over a thousand offices, so a 3-office
        # checkpoint must not pass when the expectation is not overridden.
        self.assertGreater(report.expected_offices, 1000)
        self.assertNotEqual(report.failures, [])


class SummarizeCsvTests(CompletenessTestCase):
    def test_summary_counts_offices_rows_months_and_scopes(self):
        self.write_csv(
            [
                make_record(),
                make_record(vehicle_class="MOTOR CYCLE"),
                make_record(state_code="KL", rto_code=13, month=9),
            ]
        )
        summary = summarize_csv(self.csv_path)
        self.assertEqual(summary.rows, 3)
        self.assertEqual(summary.offices, 2)
        self.assertEqual(summary.months, {9, TARGET_MONTH})
        self.assertEqual(summary.status_scopes, {STATUS_SCOPE_ALL})

    def test_offices_are_counted_per_state_and_code(self):
        # rto_code is only unique within a state; counting bare codes would
        # collapse MH/1 and KL/1 into one office.
        self.write_csv(
            [make_record(), make_record(state_code="KL", state="Kerala")]
        )
        self.assertEqual(summarize_csv(self.csv_path).offices, 2)


class MainTests(CompletenessTestCase):
    def run_main(self, extra=()):
        argv = [
            "--year",
            str(YEAR),
            "--target-month",
            str(TARGET_MONTH),
            "--output-dir",
            str(self.directory),
            "--expected-offices",
            str(len(self.labels)),
            *extra,
        ]
        return main(argv)

    def test_main_returns_zero_on_clean_run(self):
        self.set_up_clean_run()
        self.assertEqual(self.run_main(), 0)

    def test_main_returns_one_on_any_failure(self):
        self.set_up_clean_run()
        self.write_manifest()
        self.assertEqual(self.run_main(), 1)

    def test_main_honours_min_fetched_at(self):
        self.set_up_clean_run()
        stale = time.time() - 7200
        os.utime(self.csv_path, (stale, stale))
        self.assertEqual(
            self.run_main(extra=["--min-fetched-at", str(int(time.time() - 60))]),
            1,
        )


if __name__ == "__main__":
    unittest.main()
