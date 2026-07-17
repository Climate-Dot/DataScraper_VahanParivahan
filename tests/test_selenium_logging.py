import importlib.util
import json
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


def load_module(relative_path: str, module_name: str):
    module_path = REPO_ROOT / relative_path
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class DummyDriver:
    current_url = "https://example.test/report"
    title = "Example Report"
    page_source = "<html><body>broken page</body></html>"

    def get(self, url):
        self.current_url = url

    def save_screenshot(self, path):
        Path(path).write_bytes(b"png")
        return True


class DummyChromeOptions:
    def __init__(self):
        self.browser_version = None
        self.arguments = []
        self.experimental_options = {}
        self.capabilities = {}

    def add_argument(self, value):
        self.arguments.append(value)

    def add_experimental_option(self, key, value):
        self.experimental_options[key] = value

    def set_capability(self, key, value):
        self.capabilities[key] = value


class SeleniumLoggingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        install_selenium_stubs()
        cls.module = load_module("utils.py", "utils_for_selenium_logging_tests")

    def test_summarize_exception_strips_stacktrace_noise(self):
        exc = RuntimeError(
            "Message:\nElement not found xpath: //label[text()='Fuel']\n"
            "Stacktrace:\n#0 0xdeadbeef <unknown>\n#1 0xbeefdead <unknown>"
        )

        summary = self.module.summarize_exception(exc)

        self.assertEqual(
            summary,
            "RuntimeError: Element not found xpath: //label[text()='Fuel']",
        )

    def test_open_page_detects_blocked_page_and_raises_blocked_error(self):
        class BlockedDriver(DummyDriver):
            title = "Access Forbidden"
            page_source = (
                "<html><head><title>Access Forbidden</title></head>"
                "<body>You don’t have permission to access this page</body></html>"
            )

        with tempfile.TemporaryDirectory() as tmpdir:
            with mock.patch.object(self.module.os, "getcwd", return_value=tmpdir):
                with self.assertRaises(self.module.BlockedPageError) as error_context:
                    self.module.open_page(
                        BlockedDriver(),
                        "https://blocked.example.test/report",
                        step="initial_page_load",
                        context={"pipeline": "oem", "action": "load_vehicle_categories"},
                    )

                error = error_context.exception
                diagnostics = error.diagnostics

                self.assertTrue(error.blocked_by_site)
                self.assertEqual(error.page_title, "Access Forbidden")
                self.assertEqual(
                    error.blocked_reason,
                    "page_title_access_forbidden",
                )
                self.assertTrue(Path(diagnostics["metadata_path"]).exists())

                metadata = json.loads(Path(diagnostics["metadata_path"]).read_text())
                self.assertEqual(metadata["title"], "Access Forbidden")
                self.assertEqual(
                    metadata["exception"],
                    "RuntimeError: blocked_by_site=true page_title=Access Forbidden blocked_reason=page_title_access_forbidden",
                )

    def test_find_element_writes_debug_artifacts_and_raises_step_error(self):
        class FailingWebDriverWait:
            def __init__(self, *args, **kwargs):
                pass

            def until(self, condition):
                raise RuntimeError(
                    "Message:\nElement not found xpath: //label[text()='Fuel']\n"
                    "Stacktrace:\n#0 0xdeadbeef <unknown>"
                )

        with tempfile.TemporaryDirectory() as tmpdir:
            with mock.patch.object(
                self.module, "WebDriverWait", FailingWebDriverWait
            ), mock.patch.object(self.module.os, "getcwd", return_value=tmpdir):
                with self.assertRaises(self.module.SeleniumStepError) as error_context:
                    self.module.find_element(
                        DummyDriver(),
                        "xpath",
                        "//label[text()='Fuel']",
                        step="open_x_axis_dropdown",
                        context={
                            "pipeline": "rto",
                            "state": "West Bengal",
                            "month": "JUN",
                            "year": "2026",
                        },
                    )

                error = error_context.exception
                diagnostics = error.diagnostics

                self.assertEqual(error.step, "open_x_axis_dropdown")
                self.assertEqual(error.identifier, "xpath")
                self.assertEqual(error.value, "//label[text()='Fuel']")
                self.assertEqual(diagnostics["context"]["pipeline"], "rto")
                self.assertEqual(diagnostics["context"]["state"], "West Bengal")
                self.assertTrue(Path(diagnostics["screenshot_path"]).exists())
                self.assertTrue(Path(diagnostics["html_path"]).exists())
                self.assertTrue(Path(diagnostics["metadata_path"]).exists())

                metadata = json.loads(Path(diagnostics["metadata_path"]).read_text())
                self.assertEqual(metadata["step"], "open_x_axis_dropdown")
                self.assertEqual(metadata["identifier"], "xpath")
                self.assertEqual(metadata["value"], "//label[text()='Fuel']")
                self.assertEqual(metadata["context"]["month"], "JUN")
                self.assertEqual(metadata["url"], "https://example.test/report")
                self.assertEqual(
                    metadata["exception"],
                    "RuntimeError: Element not found xpath: //label[text()='Fuel']",
                )

    def test_configure_chrome_options_defaults_to_headless(self):
        options = DummyChromeOptions()

        with mock.patch.dict(self.module.os.environ, {}, clear=True):
            self.module.configure_chrome_options(
                options,
                download_directory="/tmp/report-download",
            )

        self.assertEqual(options.browser_version, "stable")
        self.assertIn("--headless", options.arguments)
        self.assertIn("--no-sandbox", options.arguments)
        self.assertIn("--disable-dev-shm-usage", options.arguments)
        self.assertEqual(
            options.experimental_options["prefs"]["download.default_directory"],
            "/tmp/report-download",
        )
        self.assertFalse(
            options.experimental_options["prefs"]["download.prompt_for_download"]
        )
        self.assertEqual(
            options.capabilities["goog:loggingPrefs"],
            {"performance": "ALL"},
        )

    def test_configure_chrome_options_respects_headless_override(self):
        options = DummyChromeOptions()

        with mock.patch.dict(
            self.module.os.environ,
            {self.module.VAHAN_HEADLESS_ENV_VAR: "false"},
            clear=True,
        ):
            self.module.configure_chrome_options(options)

        self.assertNotIn("--headless", options.arguments)

    def test_is_valid_excel_download_rejects_zero_byte_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "reportTable.xlsx"
            path.write_bytes(b"")

            self.assertFalse(self.module.is_valid_excel_download(path))

    def test_wait_for_expected_download_returns_when_valid_xlsx_exists(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "reportTable.xlsx"
            with zipfile.ZipFile(path, "w") as archive:
                archive.writestr("[Content_Types].xml", "<Types/>")
                archive.writestr("xl/workbook.xml", "<workbook/>")

            resolved = self.module.wait_for_expected_download(
                tmpdir,
                timeout_seconds=1,
                poll_interval_seconds=0.01,
                stable_seconds=0,
            )

            self.assertEqual(resolved, str(path))


if __name__ == "__main__":
    unittest.main()
