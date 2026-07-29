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
