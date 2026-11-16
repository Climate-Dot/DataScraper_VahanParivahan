"""Text-level invariants on the V2 cron entrypoint.

Cheap tests, but they are what stops a future edit from quietly reordering the
stages. The gate sitting before the snapshot is the whole safety property: if
the snapshot ran first, a partial CSV would become the newest blob and would
then satisfy the off-VM watchdog, masking the failure it exists to report.
"""

import os
import unittest
from pathlib import Path

REPO_ROOT = Path(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
ENTRYPOINT = REPO_ROOT / "new_portal_rto_monthly.sh"
SWEEP = REPO_ROOT / "ops" / "new_portal_sweep_until_complete.sh"


class EntrypointTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.raw = ENTRYPOINT.read_bytes()
        cls.text = cls.raw.decode("utf-8")

    def executable_lines(self):
        """Every line that bash would run, with comments and blanks dropped."""
        return [
            line
            for line in self.text.splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        ]

    def index_of(self, needle):
        position = self.text.find(needle)
        self.assertNotEqual(position, -1, msg=f"{needle!r} missing from the entrypoint")
        return position


class StageOrderTests(EntrypointTestCase):
    def test_stages_appear_in_required_order(self):
        positions = [
            self.index_of('CURRENT_STEP="fetch_sweep"'),
            self.index_of('CURRENT_STEP="completeness_gate"'),
            self.index_of('CURRENT_STEP="blob_snapshot"'),
            self.index_of('CURRENT_STEP="ingestion"'),
        ]
        self.assertEqual(positions, sorted(positions))

    def test_gate_precedes_snapshot_and_ingestion(self):
        gate = self.index_of("new_portal.rto_completeness")
        self.assertLess(gate, self.index_of("new_portal.rto_blob_snapshot"))
        self.assertLess(gate, self.index_of("new_portal.rto_bulk_ingest"))

    def test_post_ingest_check_runs_after_ingestion_and_before_success_alert(self):
        ingestion = self.index_of('CURRENT_STEP="ingestion"')
        check = self.index_of('CURRENT_STEP="post_ingest_check"')
        success = self.index_of("send_success_alert")
        self.assertLess(ingestion, check)
        self.assertLess(check, success)

    def test_staging_cleanup_is_the_last_stage_before_the_success_alert(self):
        check = self.index_of('CURRENT_STEP="post_ingest_check"')
        cleanup = self.index_of('CURRENT_STEP="staging_cleanup"')
        success = self.index_of("send_success_alert")
        # After the verification, so a failed check leaves the loaded staging
        # rows available for diagnosis.
        self.assertLess(check, cleanup)
        self.assertLess(cleanup, success)


class AlertingTests(EntrypointTestCase):
    def test_sets_run_label_from_describe_run_args(self):
        self.assertIn("RUN_LABEL=", self.text)
        self.assertIn("describe_run_args", self.text)

    def test_installs_failure_alert_trap_and_sends_success_alert(self):
        self.assertIn("install_failure_alert_trap", self.text)
        self.assertIn("send_success_alert", self.text)

    def test_names_the_pipeline_for_alert_routing(self):
        self.assertIn('ETL_PIPELINE_NAME="new_portal_rto"', self.text)


class ForbiddenFlagTests(EntrypointTestCase):
    def test_never_passes_allow_gaps_or_limit_rtos_or_status_scopes(self):
        # Each of these makes "complete" mean something narrower than the gate
        # assumes, so a run using them could pass the gate while short.
        for flag in ("--allow-gaps", "--limit-rtos", "--status-scopes", "--states"):
            for line in self.executable_lines():
                self.assertNotIn(flag, line)

    def test_has_no_dbt_stage(self):
        # There is no curated V2 model, so a dbt stage would fail on a model
        # that does not exist.
        for line in self.executable_lines():
            self.assertNotIn("dbt run", line)

    def test_does_not_shell_out_to_the_single_pass_etl_script(self):
        # new_portal_rto_etl.sh re-fetches from zero as its first stage and
        # would overwrite the complete CSV the sweep just produced. Explaining
        # that in a comment is wanted; invoking it is not, so only executable
        # lines are checked.
        for line in self.executable_lines():
            self.assertNotIn("new_portal_rto_etl.sh", line)


class GateWiringTests(EntrypointTestCase):
    def test_passes_min_fetched_at_to_the_gate(self):
        self.assertIn("--min-fetched-at", self.text)
        self.assertIn("RUN_START_EPOCH", self.text)
        # The floor has to be taken before the sweep, or a leftover CSV written
        # during the sweep would look like this run's output.
        self.assertLess(
            self.index_of("RUN_START_EPOCH="),
            self.index_of('CURRENT_STEP="fetch_sweep"'),
        )

    def test_sweep_is_paced_below_the_portal_token_bucket(self):
        self.assertIn("SLEEP_SECONDS=0.45", self.text)

    def test_sweep_budget_is_explicit_not_inherited(self):
        for knob in ("MAX=30", "WAIT=300", "STALL_LIMIT=6"):
            self.assertIn(knob, self.text)

    def test_ingestion_falls_back_to_the_row_by_row_path(self):
        self.assertIn("new_portal.rto_bulk_ingest", self.text)
        self.assertIn("new_portal.rto_ingest", self.text)
        self.assertLess(
            self.index_of("new_portal.rto_bulk_ingest"),
            self.index_of("new_portal.rto_ingest --year"),
        )


class FileShapeTests(EntrypointTestCase):
    def test_shebang_is_the_first_bytes_of_the_file(self):
        # new_portal_rto_etl.sh begins with a stray `e` before its shebang.
        self.assertTrue(
            self.raw.startswith(b"#!/bin/bash"),
            msg=f"file starts with {self.raw[:16]!r}",
        )

    def test_fails_fast(self):
        self.assertIn("set -eE", self.text)


class SweepPacingPassThroughTests(unittest.TestCase):
    def test_sweep_accepts_a_sleep_seconds_override(self):
        text = SWEEP.read_text(encoding="utf-8")
        self.assertIn("SLEEP_SECONDS=${SLEEP_SECONDS:-0.45}", text)
        self.assertIn('--sleep-seconds "$SLEEP_SECONDS"', text)


if __name__ == "__main__":
    unittest.main()
