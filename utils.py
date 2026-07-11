import json
import logging
import os
import re

from pipeline_constants import STATE_LIST


from datetime import datetime, timedelta
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

# configure logging to write to both the console and a file
logging.basicConfig(
    level=logging.INFO,  # set the logging level (INFO, DEBUG, etc.)
    format='%(asctime)s - %(levelname)s - %(message)s',  # log message format
)

SELENIUM_DEBUG_ROOT = os.path.join("debug_artifacts", "selenium")
VAHAN_DASHBOARD_URL = (
    "https://vahan.parivahan.gov.in/vahan4dashboard/vahan/view/reportview.xhtml"
)

state_lst = STATE_LIST

month_mapping = {
            "JAN": 1,
            "FEB": 2,
            "MAR": 3,
            "APR": 4,
            "MAY": 5,
            "JUN": 6,
            "JUL": 7,
            "AUG": 8,
            "SEP": 9,
            "OCT": 10,
            "NOV": 11,
            "DEC": 12,
        }

def get_year_month_label():
    """
    Get month and year label for OEM scraper.
    :return:
    """
    # Get the current date
    current_date = datetime.now()
    # Calculate the first day of the current month
    first_day_of_current_month = current_date.replace(day=1)
    # Calculate the last day of the previous month
    last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
    # Get the month abbreviation and year for the last day of the previous month
    month_abbreviation = last_day_of_previous_month.strftime("%b")
    year = last_day_of_previous_month.strftime("%Y")
    return month_abbreviation.upper(), year

def create_directory_if_not_exists(directory_path):
    """
    Create a new directory if it does not exist at a given location
    :param directory_path:  path to directory
    :return:
    """
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        logging.info(f"Directory '{directory_path}' created.")


class SeleniumStepError(RuntimeError):
    """Raised when a Selenium page interaction fails after diagnostics are captured."""

    def __init__(
        self,
        step,
        identifier,
        value,
        context,
        original_exception,
        diagnostics=None,
    ):
        self.step = step or "unknown_step"
        self.identifier = identifier
        self.value = value
        self.context = context or {}
        self.original_exception = original_exception
        self.diagnostics = diagnostics or {}

        context_text = format_log_context(self.context)
        message = (
            f"Selenium step failed step={self.step} "
            f"locator={self.identifier}:{self.value}"
        )
        if context_text:
            message += f" context={context_text}"
        message += f" error={summarize_exception(original_exception)}"

        super().__init__(message)


def sanitize_artifact_label(value, max_length=80):
    sanitized = re.sub(r"[^A-Za-z0-9._-]+", "_", str(value or "")).strip("_")
    if not sanitized:
        sanitized = "unknown"
    return sanitized[:max_length]


def format_log_context(context):
    if not context:
        return ""

    return ", ".join(
        f"{key}={value}"
        for key, value in sorted(context.items())
        if value not in (None, "")
    )


def summarize_exception(exc):
    lines = [
        line.strip()
        for line in str(exc).splitlines()
        if line.strip()
        and line.strip() not in {"Message:", "Stacktrace:"}
        and not line.strip().startswith("#")
    ]
    summary = lines[0] if lines else exc.__class__.__name__
    return f"{exc.__class__.__name__}: {summary}"


def capture_browser_diagnostics(driver, step, identifier, value, context, exc):
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S_%f")
    pipeline = sanitize_artifact_label((context or {}).get("pipeline", "unknown"))
    artifact_dir = os.path.join(os.getcwd(), SELENIUM_DEBUG_ROOT, pipeline)
    create_directory_if_not_exists(artifact_dir)

    base_name = "_".join(
        [
            timestamp,
            sanitize_artifact_label(step or "unknown_step"),
            sanitize_artifact_label((context or {}).get("state", "no_state")),
        ]
    )

    screenshot_path = os.path.join(artifact_dir, f"{base_name}.png")
    html_path = os.path.join(artifact_dir, f"{base_name}.html")
    metadata_path = os.path.join(artifact_dir, f"{base_name}.json")

    diagnostics = {
        "timestamp": timestamp,
        "step": step,
        "identifier": identifier,
        "value": value,
        "context": context or {},
        "url": getattr(driver, "current_url", ""),
        "title": getattr(driver, "title", ""),
        "exception": summarize_exception(exc),
        "screenshot_path": screenshot_path,
        "html_path": html_path,
        "metadata_path": metadata_path,
    }

    try:
        if hasattr(driver, "save_screenshot"):
            driver.save_screenshot(screenshot_path)
    except Exception as screenshot_exc:
        diagnostics["screenshot_error"] = summarize_exception(screenshot_exc)

    try:
        page_source = getattr(driver, "page_source", "")
        with open(html_path, "w", encoding="utf-8") as html_file:
            html_file.write(page_source)
    except Exception as html_exc:
        diagnostics["html_error"] = summarize_exception(html_exc)

    try:
        with open(metadata_path, "w", encoding="utf-8") as metadata_file:
            json.dump(diagnostics, metadata_file, indent=2)
    except Exception as metadata_exc:
        diagnostics["metadata_error"] = summarize_exception(metadata_exc)

    return diagnostics


def open_page(driver, url, step="initial_page_load", context=None):
    try:
        driver.get(url)
    except Exception as e:
        diagnostics = capture_browser_diagnostics(
            driver=driver,
            step=step,
            identifier="url",
            value=url,
            context=context,
            exc=e,
        )
        logging.error(
            "Selenium page load failed step=%s url=%s context=%s diagnostics=%s error=%s",
            step,
            url,
            format_log_context(context),
            diagnostics.get("metadata_path", ""),
            summarize_exception(e),
        )
        raise SeleniumStepError(
            step=step,
            identifier="url",
            value=url,
            context=context,
            original_exception=e,
            diagnostics=diagnostics,
        ) from e


def find_element(driver, identifier, value, timeout=10, step=None, context=None):
    """
    Find and return a web element based on the identifier (ID, CSS, or XPath).

    Parameters:
    - driver: Selenium WebDriver instance.
    - identifier: String, either "id", "css", or "xpath".
    - value: String, the value of the ID, CSS selector, or XPath expression.
    - timeout: Optional, timeout for WebDriverWait in seconds. Default is 20 seconds.
    - step: Optional semantic step name for better failure logs.
    - context: Optional dict with pipeline metadata like state/month/year.

    Returns:
    - WebElement: The located element.
    """
    try:
        if identifier == "id":
            element = WebDriverWait(driver, timeout).until(
                EC.element_to_be_clickable((By.ID, value))
            )
        elif identifier == "css":
            element = WebDriverWait(driver, timeout).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, value))
            )
        elif identifier == "xpath":
            element = WebDriverWait(driver, timeout).until(
                EC.element_to_be_clickable((By.XPATH, value))
            )
        elif identifier == "tag":
            element = WebDriverWait(driver, timeout).until(
                EC.element_to_be_clickable((By.TAG_NAME, value))
            )
        else:
            raise ValueError(
                "Invalid identifier. Use 'id', 'css', 'tag' or 'xpath'."
            )
        return element
    except Exception as e:
        diagnostics = capture_browser_diagnostics(
            driver=driver,
            step=step,
            identifier=identifier,
            value=value,
            context=context,
            exc=e,
        )
        logging.error(
            "Selenium element lookup failed step=%s locator=%s:%s context=%s url=%s diagnostics=%s error=%s",
            step or "unknown_step",
            identifier,
            value,
            format_log_context(context),
            diagnostics.get("url", ""),
            diagnostics.get("metadata_path", ""),
            summarize_exception(e),
        )
        raise SeleniumStepError(
            step=step,
            identifier=identifier,
            value=value,
            context=context,
            original_exception=e,
            diagnostics=diagnostics,
        ) from e

def convert_date(date_str):
    return datetime.strptime(date_str, "%d/%b/%Y").strftime("%d/%m/%Y")

def remove_special_chars(text):
    return re.sub(r"\W+", " ", text)
