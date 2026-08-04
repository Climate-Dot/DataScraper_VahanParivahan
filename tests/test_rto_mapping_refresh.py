import importlib.util
import unittest
from pathlib import Path

from tests._selenium_test_stubs import install_selenium_stubs


REPO_ROOT = Path(__file__).resolve().parents[1]


def load_module(relative_path: str, module_name: str):
    module_path = REPO_ROOT / relative_path
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class RtoMappingRefreshTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        install_selenium_stubs()
        cls.module = load_module("rto_level/rto_level_data_scraper.py", "rto_scraper")

    def test_merge_prefers_fresh_mapping_and_keeps_previous_fallback(self):
        previous = {
            "Andhra Pradesh": ["Old AP Office"],
            "Telangana": ["Old TS Office"],
        }
        fresh = {
            "Andhra Pradesh": ["New AP Office"],
        }

        merged = self.module.merge_state_rto_mappings(previous, fresh)

        self.assertEqual(merged["Andhra Pradesh"], ["New AP Office"])
        self.assertEqual(merged["Telangana"], ["Old TS Office"])

    def test_missing_mapping_states_treats_empty_and_absent_as_missing(self):
        mapping = {
            "Andhra Pradesh": ["AP Office"],
            "Telangana": [],
        }

        missing = self.module.get_missing_mapping_states(
            mapping,
            ["Andhra Pradesh", "Telangana", "Kerala"],
        )

        self.assertEqual(missing, ["Telangana", "Kerala"])

    def test_build_rto_folder_name_sanitizes_slashes(self):
        folder_name = self.module.RTODataScraper.build_rto_folder_name(
            "RAJPUR ROAD/VIU BURARI - DL51( 08-APR-2016 )"
        )

        self.assertEqual(folder_name, "RAJPUR ROADVIU BURARI_DL51")

    def test_build_rto_option_xpath_disambiguates_prefix_colliding_codes(self):
        # Regression test for the 2026-07 production incident: a bare
        # contains(text(), code) selector let a shorter code (e.g. "HR2")
        # match a longer, unrelated office's rendered label (e.g. "HR29",
        # "HR269") whenever the shorter code happened to be a text prefix of
        # the longer one, causing the wrong office's report to be
        # downloaded. These are real labels pulled from the live RTO
        # mapping for Haryana.
        candidate_labels = [
            "JAGADHARI - HR2( 06-JUN-2017 )",
            "BALLABGARH - HR29( 18-JUL-2017 )",
            "SDM GURUGRAM - HR260( 25-APR-2022 )",
            "M/S M.G. MOTORS - HR269( 12-OCT-2021 )",
        ]

        xpath = self.module.RTODataScraper.build_rto_option_xpath("HR2")
        # Simulate what the XPath's contains(text(), "...") would match
        # against each candidate's rendered text.
        search_fragment = xpath.split('contains(text(), "')[1].rstrip('")]')
        matches = [label for label in candidate_labels if search_fragment in label]

        self.assertEqual(matches, ["JAGADHARI - HR2( 06-JUN-2017 )"])

        # Confirm this genuinely regresses without the fix: the old bare
        # code fragment would have matched all four candidates.
        old_matches = [label for label in candidate_labels if "HR2" in label]
        self.assertEqual(len(old_matches), 4)

    def test_build_download_directory_supports_custom_root(self):
        directory = self.module.RTODataScraper.build_download_directory(
            "Telangana",
            "Hyderabad RTO - TG01( 01-JAN-2026 )",
            "2026",
            "JUN",
            download_root="/tmp/telangana-backfill",
        )

        self.assertEqual(
            directory,
            "/tmp/telangana-backfill/Telangana/Hyderabad RTO_TG01/2026/JUN",
        )


if __name__ == "__main__":
    unittest.main()
