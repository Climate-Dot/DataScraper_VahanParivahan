import importlib.util
import sys
import types
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def install_selenium_stubs():
    selenium = types.ModuleType("selenium")
    webdriver = types.ModuleType("selenium.webdriver")
    common = types.ModuleType("selenium.common")
    exceptions = types.ModuleType("selenium.common.exceptions")
    webdriver_common = types.ModuleType("selenium.webdriver.common")
    webdriver_common_by = types.ModuleType("selenium.webdriver.common.by")
    webdriver_support = types.ModuleType("selenium.webdriver.support")
    webdriver_support_ec = types.ModuleType(
        "selenium.webdriver.support.expected_conditions"
    )
    webdriver_support_wait = types.ModuleType("selenium.webdriver.support.wait")
    webdriver_chrome = types.ModuleType("selenium.webdriver.chrome")
    webdriver_chrome_service = types.ModuleType("selenium.webdriver.chrome.service")
    webdriver_manager = types.ModuleType("webdriver_manager")
    webdriver_manager_chrome = types.ModuleType("webdriver_manager.chrome")

    class DummyException(Exception):
        pass

    class DummyBy:
        TAG_NAME = "tag_name"

    class DummyWebDriverWait:
        def __init__(self, *args, **kwargs):
            pass

        def until(self, condition):
            return condition

    def dummy_clickable(locator):
        return locator

    class DummyService:
        def __init__(self, *args, **kwargs):
            pass

    class DummyChromeDriverManager:
        def install(self):
            return "/tmp/chromedriver"

    exceptions.TimeoutException = DummyException
    exceptions.StaleElementReferenceException = DummyException
    exceptions.WebDriverException = DummyException
    webdriver_common_by.By = DummyBy
    webdriver_support_ec.element_to_be_clickable = dummy_clickable
    webdriver_support_wait.WebDriverWait = DummyWebDriverWait
    webdriver_chrome_service.Service = DummyService
    webdriver_manager_chrome.ChromeDriverManager = DummyChromeDriverManager

    sys.modules.setdefault("selenium", selenium)
    sys.modules.setdefault("selenium.webdriver", webdriver)
    sys.modules.setdefault("selenium.common", common)
    sys.modules.setdefault("selenium.common.exceptions", exceptions)
    sys.modules.setdefault("selenium.webdriver.common", webdriver_common)
    sys.modules.setdefault("selenium.webdriver.common.by", webdriver_common_by)
    sys.modules.setdefault("selenium.webdriver.support", webdriver_support)
    sys.modules.setdefault(
        "selenium.webdriver.support.expected_conditions",
        webdriver_support_ec,
    )
    sys.modules.setdefault("selenium.webdriver.support.wait", webdriver_support_wait)
    sys.modules.setdefault("selenium.webdriver.chrome", webdriver_chrome)
    sys.modules.setdefault(
        "selenium.webdriver.chrome.service",
        webdriver_chrome_service,
    )
    sys.modules.setdefault("webdriver_manager", webdriver_manager)
    sys.modules.setdefault("webdriver_manager.chrome", webdriver_manager_chrome)


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
