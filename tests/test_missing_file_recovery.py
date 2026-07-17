import importlib.util
import sys
import tempfile
import types
import unittest
import zipfile
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[1]


def install_selenium_stubs():
    selenium = types.ModuleType("selenium")
    webdriver = types.ModuleType("selenium.webdriver")
    webdriver_common = types.ModuleType("selenium.webdriver.common")
    webdriver_common_by = types.ModuleType("selenium.webdriver.common.by")
    webdriver_support = types.ModuleType("selenium.webdriver.support")
    webdriver_support_ec = types.ModuleType(
        "selenium.webdriver.support.expected_conditions"
    )
    webdriver_support_wait = types.ModuleType("selenium.webdriver.support.wait")

    class DummyBy:
        ID = "id"
        CSS_SELECTOR = "css"
        XPATH = "xpath"
        TAG_NAME = "tag_name"

    class DummyWebDriverWait:
        def __init__(self, *args, **kwargs):
            pass

        def until(self, condition):
            return condition

    def dummy_clickable(locator):
        return locator

    webdriver_common_by.By = DummyBy
    webdriver_support_ec.element_to_be_clickable = dummy_clickable
    webdriver_support_wait.WebDriverWait = DummyWebDriverWait

    sys.modules.setdefault("selenium", selenium)
    sys.modules.setdefault("selenium.webdriver", webdriver)
    sys.modules.setdefault("selenium.webdriver.common", webdriver_common)
    sys.modules.setdefault("selenium.webdriver.common.by", webdriver_common_by)
    sys.modules.setdefault("selenium.webdriver.support", webdriver_support)
    sys.modules.setdefault(
        "selenium.webdriver.support.expected_conditions",
        webdriver_support_ec,
    )
    sys.modules.setdefault("selenium.webdriver.support.wait", webdriver_support_wait)


def load_module(relative_path, module_name, stub_modules=None):
    previous_modules = {}
    stub_modules = stub_modules or {}

    for name, module in stub_modules.items():
        previous_modules[name] = sys.modules.get(name)
        sys.modules[name] = module

    try:
        module_path = REPO_ROOT / relative_path
        spec = importlib.util.spec_from_file_location(module_name, module_path)
        module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(module)
        return module
    finally:
        for name, previous_module in previous_modules.items():
            if previous_module is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = previous_module


def write_valid_xlsx(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("[Content_Types].xml", "<Types/>")
        archive.writestr("xl/workbook.xml", "<workbook/>")


class MissingFileRecoveryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        install_selenium_stubs()

    def test_state_build_missing_parameters_skips_valid_reports(self):
        package_module = types.ModuleType("state_level")
        package_module.__path__ = []
        stub_module = types.ModuleType("state_level.state_level_data_scraper")

        class DummyStateLevelDataScraper:
            pass

        stub_module.StateLevelDataScraper = DummyStateLevelDataScraper
        module = load_module(
            "state_level/state_level_get_missing_files.py",
            "state_missing_file_recovery",
            {
                "state_level": package_module,
                "state_level.state_level_data_scraper": stub_module,
            },
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            report_path = (
                Path(tmpdir)
                / "state_level"
                / "state_level_ev_data"
                / "Andhra Pradesh"
                / "2026"
                / "JUN"
                / "reportTable.xlsx"
            )
            write_valid_xlsx(report_path)

            with mock.patch.object(module.os, "getcwd", return_value=tmpdir):
                with mock.patch.object(
                    module, "STATE_LIST", ["Andhra Pradesh", "Telangana"]
                ):
                    parameters = module.build_missing_parameters("JUN", "2026")

        self.assertEqual(parameters, [("Telangana", "2026", "JUN")])

    def test_oem_build_missing_parameters_skips_valid_reports(self):
        package_module = types.ModuleType("oem_level")
        package_module.__path__ = []
        stub_module = types.ModuleType("oem_level.oem_level_data_scraper")

        class DummyOEMDataScraper:
            pass

        stub_module.OEMDataScraper = DummyOEMDataScraper
        module = load_module(
            "oem_level/get_missing_files.py",
            "oem_missing_file_recovery",
            {
                "oem_level": package_module,
                "oem_level.oem_level_data_scraper": stub_module,
            },
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            report_path = (
                Path(tmpdir)
                / "oem_level"
                / "oem_data_by_state_and_category"
                / "Andhra Pradesh"
                / "MOTOR CAR"
                / "2026"
                / "JUN"
                / "reportTable.xlsx"
            )
            write_valid_xlsx(report_path)

            with mock.patch.object(module.os, "getcwd", return_value=tmpdir):
                with mock.patch.object(module, "STATE_LIST", ["Andhra Pradesh"]):
                    parameters = module.build_missing_parameters(
                        ["MOTOR CAR", "BUS"], "JUN", "2026"
                    )

        self.assertEqual(parameters, [("Andhra Pradesh", "2026", "JUN", "BUS")])

    def test_rto_build_missing_parameters_flags_invalid_labels(self):
        package_module = types.ModuleType("rto_level")
        package_module.__path__ = []
        scraper_module = types.ModuleType("rto_level.rto_level_data_scraper")

        class DummyRTODataScraper:
            @staticmethod
            def build_rto_folder_name(rto_label):
                if rto_label == "Bad Label":
                    return None
                return "Sample Office_AP01"

        scraper_module.RTODataScraper = DummyRTODataScraper
        module = load_module(
            "rto_level/rto_level_get_missing_files.py",
            "rto_missing_file_recovery",
            {
                "rto_level": package_module,
                "rto_level.rto_level_data_scraper": scraper_module,
            },
        )

        mapping = {"Andhra Pradesh": ["Good Label", "Bad Label"]}
        parameters, invalid_labels = module.build_missing_parameters(
            DummyRTODataScraper(), mapping, "JUN", "2026"
        )

        self.assertEqual(parameters, [("Andhra Pradesh", "Good Label", "2026", "JUN")])
        self.assertEqual(invalid_labels, [("Andhra Pradesh", "Bad Label")])

    def test_rto_count_existing_valid_files_counts_only_valid_workbooks(self):
        package_module = types.ModuleType("rto_level")
        package_module.__path__ = []
        scraper_module = types.ModuleType("rto_level.rto_level_data_scraper")

        class DummyRTODataScraper:
            @staticmethod
            def build_rto_folder_name(rto_label):
                return "Sample Office_AP01"

        scraper_module.RTODataScraper = DummyRTODataScraper
        module = load_module(
            "rto_level/rto_level_get_missing_files.py",
            "rto_missing_file_counts",
            {
                "rto_level": package_module,
                "rto_level.rto_level_data_scraper": scraper_module,
            },
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            report_path = (
                Path(tmpdir)
                / "rto_level"
                / "rto_level_ev_data"
                / "Andhra Pradesh"
                / "Sample Office_AP01"
                / "2026"
                / "JUN"
                / "reportTable.xlsx"
            )
            write_valid_xlsx(report_path)

            with mock.patch.object(module.os, "getcwd", return_value=tmpdir):
                with mock.patch.object(module, "STATE_LIST", ["Andhra Pradesh"]):
                    count = module.count_existing_valid_files(
                        DummyRTODataScraper(),
                        {"Andhra Pradesh": ["Good Label"]},
                        "JUN",
                        "2026",
                    )

        self.assertEqual(count, 1)


if __name__ == "__main__":
    unittest.main()
