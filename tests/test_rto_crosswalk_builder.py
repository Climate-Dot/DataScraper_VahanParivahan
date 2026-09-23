import csv
import logging
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from new_portal import build_rto_crosswalk as builder


class DeriveLegacyCodeTests(unittest.TestCase):
    def test_plain_state_prefixes_the_numeric_code(self):
        self.assertEqual(builder.derive_legacy_code("MH", 12), "MH12")

    def test_odisha_uses_the_od_prefix_not_the_portal_or_code(self):
        # The one prefix alias across all 36 states; see the module docstring.
        self.assertEqual(builder.derive_legacy_code("OR", 1), "OD1")

    def test_accepts_string_codes(self):
        self.assertEqual(builder.derive_legacy_code("HR", "2"), "HR2")


class NameSuffixTests(unittest.TestCase):
    def test_strips_the_embedded_code_suffix(self):
        self.assertEqual(builder.strip_code_suffix("PUNE - MH12"), "PUNE")

    def test_leaves_a_name_without_a_suffix_untouched(self):
        self.assertEqual(builder.strip_code_suffix("PUNE"), "PUNE")

    def test_keeps_hyphens_that_are_part_of_the_name(self):
        self.assertEqual(
            builder.strip_code_suffix("RLA SHIMLA HP-03/HP-07(URBAN) - HP3"),
            "RLA SHIMLA HP-03/HP-07(URBAN)",
        )

    def test_extracts_the_embedded_code(self):
        self.assertEqual(builder.embedded_code("PUNE - MH12"), "MH12")
        self.assertIsNone(builder.embedded_code("PUNE"))


class VerifyEmbeddedCodeTests(unittest.TestCase):
    def test_agreeing_code_is_silent(self):
        with self.assertNoLogs(builder.logger, level=logging.WARNING):
            builder.verify_embedded_code("MH", {"rtoCode": 12, "rtoName": "PUNE - MH12"})

    def test_disagreeing_code_warns(self):
        # Guards against the portal renumbering underneath the arithmetic rule.
        with self.assertLogs(builder.logger, level=logging.WARNING) as captured:
            builder.verify_embedded_code("MH", {"rtoCode": 12, "rtoName": "PUNE - MH99"})
        self.assertIn("MH99", captured.output[0])

    def test_missing_suffix_warns(self):
        with self.assertLogs(builder.logger, level=logging.WARNING):
            builder.verify_embedded_code("MH", {"rtoCode": 12, "rtoName": "PUNE"})


def _portal(rtos_by_state):
    portal = mock.Mock()
    portal.get_rtos_for_state.side_effect = lambda sc: rtos_by_state.get(sc, [])
    return portal


class BuildCrosswalkRowsTests(unittest.TestCase):
    def test_links_portal_rtos_to_legacy_history(self):
        legacy = [("Maharashtra", "MH12", "PUNE")]
        portal = _portal({"MH": [{"rtoCode": 12, "rtoName": "PUNE - MH12"}]})

        rows = [r for r in builder.build_crosswalk_rows(legacy, portal) if r["state_code"] == "MH"]

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["legacy_rto_code"], "MH12")
        self.assertEqual(rows[0]["new_rto_code"], 12)
        self.assertEqual(rows[0]["new_rto_name"], "PUNE")
        self.assertEqual(rows[0]["link_status"], builder.LINK_LINKED)

    def test_portal_rto_with_no_v1_history_is_kept_as_portal_only(self):
        # V2 ingests forward from the live list, so these must not be dropped.
        portal = _portal({"MH": [{"rtoCode": 99, "rtoName": "BRAND NEW - MH99"}]})

        rows = [r for r in builder.build_crosswalk_rows([], portal) if r["state_code"] == "MH"]

        self.assertEqual(rows[0]["link_status"], builder.LINK_PORTAL_ONLY)
        self.assertEqual(rows[0]["legacy_rto_code"], "MH99")
        self.assertEqual(rows[0]["legacy_rto_name"], "")

    def test_legacy_rto_absent_from_the_portal_is_kept_as_legacy_only(self):
        legacy = [("Mizoram", "MZ10", "KHAWZAWL")]
        portal = _portal({})

        rows = [r for r in builder.build_crosswalk_rows(legacy, portal) if r["legacy_rto_code"] == "MZ10"]

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["link_status"], builder.LINK_LEGACY_ONLY)
        self.assertEqual(rows[0]["new_rto_code"], "")

    def test_renamed_code_contributes_one_row_not_two(self):
        # The 24 known office-rename cases: one code, two historical names.
        legacy = [
            ("Delhi", "DL5", "OLD NAME NOBODY USES ANYMORE"),
            ("Delhi", "DL5", "CURRENT OFFICE NAME"),
        ]
        portal = _portal({"DL": [{"rtoCode": 5, "rtoName": "CURRENT OFFICE NAME - DL5"}]})

        rows = [r for r in builder.build_crosswalk_rows(legacy, portal) if r["legacy_rto_code"] == "DL5"]

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["link_status"], builder.LINK_LINKED)

    def test_odisha_portal_rtos_link_to_od_prefixed_legacy_codes(self):
        legacy = [("Odisha", "OD1", "BALASORE RTO")]
        portal = _portal({"OR": [{"rtoCode": 1, "rtoName": "BALASORE RTO - OR1"}]})

        rows = [r for r in builder.build_crosswalk_rows(legacy, portal) if r["state_code"] == "OR"]

        self.assertEqual(rows[0]["legacy_rto_code"], "OD1")
        self.assertEqual(rows[0]["link_status"], builder.LINK_LINKED)

    def test_one_api_call_per_state_regardless_of_rto_count(self):
        legacy = [("Maharashtra", "MH1", "MUMBAI CENTRAL"), ("Maharashtra", "MH12", "PUNE")]
        portal = _portal(
            {
                "MH": [
                    {"rtoCode": 1, "rtoName": "MUMBAI CENTRAL - MH1"},
                    {"rtoCode": 12, "rtoName": "PUNE - MH12"},
                ]
            }
        )

        builder.build_crosswalk_rows(legacy, portal)

        self.assertEqual(portal.get_rtos_for_state.call_args_list.count(mock.call("MH")), 1)


class WriteCrosswalkTests(unittest.TestCase):
    def test_writes_expected_columns(self):
        rows = builder.build_crosswalk_rows(
            [("Maharashtra", "MH12", "PUNE")],
            _portal({"MH": [{"rtoCode": 12, "rtoName": "PUNE - MH12"}]}),
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "rto_code_crosswalk.csv"
            builder.write_crosswalk(rows, output_path=output_path)
            with output_path.open(newline="", encoding="utf-8") as f:
                written = list(csv.DictReader(f))

        row = next(r for r in written if r["legacy_rto_code"] == "MH12")
        self.assertEqual(row["new_rto_code"], "12")
        self.assertEqual(row["link_status"], "linked")


class SeedFileTests(unittest.TestCase):
    """Guards on the committed seed itself, not just the builder logic."""

    @classmethod
    def setUpClass(cls):
        with builder.OUTPUT_PATH.open(newline="", encoding="utf-8") as f:
            cls.rows = list(csv.DictReader(f))

    def test_every_row_has_a_legacy_code(self):
        self.assertTrue(all(r["legacy_rto_code"] for r in self.rows))

    def test_new_portal_key_is_unique(self):
        keys = [(r["state_code"], r["new_rto_code"]) for r in self.rows if r["new_rto_code"]]
        self.assertEqual(len(keys), len(set(keys)))

    def test_legacy_code_is_unique(self):
        codes = [r["legacy_rto_code"] for r in self.rows]
        self.assertEqual(len(codes), len(set(codes)))

    def test_derived_legacy_code_matches_the_stored_one(self):
        for row in self.rows:
            if not row["new_rto_code"]:
                continue
            self.assertEqual(
                builder.derive_legacy_code(row["state_code"], row["new_rto_code"]),
                row["legacy_rto_code"],
            )


if __name__ == "__main__":
    unittest.main()
