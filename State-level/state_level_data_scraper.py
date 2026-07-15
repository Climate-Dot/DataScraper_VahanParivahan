from selenium import webdriver
from selenium.common.exceptions import (
    TimeoutException,
    StaleElementReferenceException,
    WebDriverException,
)
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from datetime import datetime, timedelta

import logging
import os
import re
import sys
import shutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

repo_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if repo_path not in sys.path:
    sys.path.append(repo_path)

from pipeline_constants import STATE_LIST
from pipeline_logging import configure_pipeline_logging
from utils import (
    BlockedPageError,
    SeleniumStepError,
    find_element as shared_find_element,
    format_log_context,
    open_page,
    summarize_exception,
    VAHAN_DASHBOARD_URL,
)

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

configure_pipeline_logging()


class StateLevelDataScraper:
    def __init__(self):
        self.max_retries = 5
        self.retry_delay = 15

    @staticmethod
    def create_directory_if_not_exists(directory_path):
        """
        Create a new directory if it does not exist at a given lo9cation
        :param directory_path:  path to directory
        :return:
        """
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)
            logging.info(f"Directory '{directory_path}' created.")

    @staticmethod
    def find_element(driver, identifier, value, timeout=10, step=None, context=None):
        return shared_find_element(
            driver,
            identifier,
            value,
            timeout=timeout,
            step=step,
            context=context,
        )

    @staticmethod
    def get_year_month_label():
        """
        Get month and year label.
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

    def extract_state_level_data(
            self, state_label, year_label, month_label
    ):
        """
        :param state_label: State label for data 
        :param year_label: Year label for data
        :param month_label: Month label for data
        :return: Downloads csv file in directory set up by chrome
        """
        browserOpts = webdriver.ChromeOptions()

        browserOpts.browser_version = "stable"
        browserPrefs = {
            "credentials_enable_service": False,
            "profile.password_manager_enabled": False,
        }
        browserOpts.add_experimental_option(
            "excludeSwitches", ["enable-automation", "enable-logging"]
        )
        browserOpts.add_experimental_option("prefs", browserPrefs)
        browserOpts.add_argument("--headless")
        browserOpts.add_argument("--no-sandbox")
        browserOpts.add_argument("--disable-dev-shm-usage")
        browserOpts.add_argument("--disable-single-click-autofill")
        browserOpts.set_capability("goog:loggingPrefs", {"performance": "ALL"})

        state_folder_name = re.sub(r"[^a-zA-Z\s]", " ", state_label).rstrip()

        download_path = os.path.join(
            os.getcwd(),
            "State-level",
            "state_level_ev_data",
            state_folder_name.rstrip(),
            str(year_label),
            month_label,
        )

        self.create_directory_if_not_exists(download_path)
        browserPrefs.update({"download.default_directory": download_path})
        browserOpts.add_experimental_option("prefs", browserPrefs)
        browser = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()), options=browserOpts
        )
        context = {
            "pipeline": "state",
            "state": state_label,
            "year": year_label,
            "month": month_label,
        }

        try:
            retries = 0
            last_exception = None
            while retries < self.max_retries:
                try:
                    open_page(
                        browser,
                        VAHAN_DASHBOARD_URL,
                        step="initial_page_load",
                        context=context,
                    )

                    self.find_element(
                        browser,
                        "xpath",
                        '//label[starts-with(text(), "All Vahan4 Running States")]',
                        step="open_state_dropdown",
                        context=context,
                    ).click()
                    time.sleep(2)

                    self.find_element(
                        browser,
                        "xpath",
                        f'//li[starts-with(text(), "{state_label}")]',
                        step="select_state",
                        context=context,
                    ).click()
                    time.sleep(2)

                    self.find_element(
                        browser,
                        "id",
                        "yaxisVar_label",
                        step="open_y_axis_dropdown",
                        context=context,
                    ).click()
                    time.sleep(2)
                    self.find_element(
                        browser,
                        "id",
                        "yaxisVar_1",
                        step="select_y_axis_vehicle_class",
                        context=context,
                    ).click()
                    time.sleep(2)

                    self.find_element(
                        browser,
                        "id",
                        "xaxisVar_label",
                        step="open_x_axis_dropdown",
                        context=context,
                    ).click()
                    time.sleep(1)
                    self.find_element(
                        browser,
                        "xpath",
                        "//ul[@id='xaxisVar_items']/li[text()='Fuel']",
                        step="select_x_axis_fuel",
                        context=context,
                    ).click()
                    time.sleep(2)

                    self.find_element(
                        browser,
                        "id",
                        "selectedYear_label",
                        step="open_year_dropdown",
                        context=context,
                    ).click()
                    time.sleep(2)
                    self.find_element(
                        browser,
                        "xpath",
                        f"//ul[@id='selectedYear_items']/li[text()='{year_label}']",
                        step="select_year",
                        context=context,
                    ).click()
                    time.sleep(5)

                    self.find_element(
                        browser,
                        "css",
                        "button[class='ui-button ui-widget ui-state-default ui-corner-all ui-button-text-icon-left button']",
                        step="click_main_refresh",
                        context=context,
                    ).click()
                    time.sleep(5)

                    self.find_element(
                        browser,
                        "id",
                        "groupingTable:selectMonth_label",
                        step="open_month_dropdown",
                        context=context,
                    ).click()
                    time.sleep(2)
                    self.find_element(
                        browser,
                        "xpath",
                        f"//ul[@id='groupingTable:selectMonth_items']/li[text()='{month_label}']",
                        step="select_month",
                        context=context,
                    ).click()
                    time.sleep(2)

                    self.find_element(
                        browser,
                        "id",
                        "groupingTable:xls",
                        step="download_report",
                        context=context,
                    ).click()
                    time.sleep(5)
                    logging.info(
                        "Downloaded state report state=%s year=%s month=%s",
                        state_folder_name,
                        year_label,
                        month_label,
                    )
                    return
                except BlockedPageError as e:
                    logging.error(
                        "State page access blocked context=%s page_title=%s diagnostics=%s error=%s",
                        format_log_context(context),
                        e.page_title,
                        e.diagnostics.get("metadata_path", ""),
                        e,
                    )
                    raise
                except (
                    SeleniumStepError,
                    TimeoutException,
                    StaleElementReferenceException,
                    WebDriverException,
                ) as e:
                    last_exception = e
                    retries += 1
                    logging.warning(
                        "Retrying state download attempt=%s/%s context=%s failed_step=%s error=%s",
                        retries,
                        self.max_retries,
                        format_log_context(context),
                        getattr(e, "step", "download_flow"),
                        summarize_exception(e),
                    )
                    time.sleep(self.retry_delay)

            raise last_exception
        finally:
            browser.quit()

    # define a function to wrap the selenium function for argument unpacking
    def run_selenium(self, args):
        return self.extract_state_level_data(*args)


def main():
    data_extract_class = StateLevelDataScraper()

    state_lst = STATE_LIST

    if len(sys.argv) > 2:
        # If month and year are passed as command-line arguments
        month = sys.argv[1]
        year = sys.argv[2]
    else:
        # Use the default function to get the month and year
        month, year = data_extract_class.get_year_month_label()

    parameters = []
    for state in state_lst:
        directory_path = os.path.join(
            os.getcwd(),
            "State-level",
            "state_level_ev_data",
            re.sub(r"[^a-zA-Z\s]", " ", state).rstrip(),
            str(year),
            month,
        )

        # remove the file if already exists from previous month
        if os.path.exists(directory_path):
            shutil.rmtree(directory_path)

        parameters.append((state, year, month))

    # Run selenium function in parallel
    logging.info(
        "Prepared state download tasks count=%s year=%s month=%s",
        len(parameters),
        year,
        month,
    )

    with ThreadPoolExecutor(
            max_workers=34
    ) as executor:  # Adjust max_workers based on your system's capability
        futures = {
            executor.submit(data_extract_class.run_selenium, args): args
            for args in parameters
        }

        successful_downloads = 0
        failed_downloads = []

        for future in as_completed(futures):
            state, year_label, month_label = futures[future]
            try:
                future.result()
                successful_downloads += 1
            except Exception as e:
                failed_downloads.append(state)
                logging.error(
                    "State download failed state=%s year=%s month=%s failed_step=%s diagnostics=%s error=%s",
                    state,
                    year_label,
                    month_label,
                    getattr(e, "step", "download_flow"),
                    getattr(e, "diagnostics", {}).get("metadata_path", ""),
                    summarize_exception(getattr(e, "original_exception", e)),
                )

    logging.info(
        "State extraction summary for %s %s: prepared=%s succeeded=%s failed=%s",
        month,
        year,
        len(parameters),
        successful_downloads,
        len(failed_downloads),
    )


if __name__ == "__main__":
    main()
