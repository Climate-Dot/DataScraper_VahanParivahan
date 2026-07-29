"""Shared fake `selenium`/`webdriver_manager` modules for loading Selenium-dependent
source files under test without the real packages installed (matches the CI
environment, which only installs requirements-ci.txt).

Call install_selenium_stubs() once before importing/loading any module that
transitively imports selenium. Uses sys.modules.setdefault, so it is safe to
call from multiple test files/classes.
"""
import sys
import types


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
