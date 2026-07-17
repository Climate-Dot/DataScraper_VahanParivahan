from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
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
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

repo_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if repo_path not in sys.path:
    sys.path.append(repo_path)

from pipeline_constants import STATE_LIST
from pipeline_logging import configure_pipeline_logging
from utils import (
    configure_chrome_options,
    is_valid_excel_download,
    wait_for_expected_download,
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
        else:
            logging.info(f"Directory '{directory_path}' already exists.")
            return

    @staticmethod
    def find_element(driver, identifier, value, timeout=10):
        """
        Find and return a web element based on the identifier (ID, CSS, or XPath).

        Parameters:
        - driver: Selenium WebDriver instance.
        - identifier: String, either "id", "css", or "xpath".
        - value: String, the value of the ID, CSS selector, or XPath expression.
        - timeout: Optional, timeout for WebDriverWait in seconds. Default is 20 seconds.

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
                    EC.element_to_be_clickable((By.TAG, value))
                )
            else:
                raise ValueError(
                    "Invalid identifier. Use 'id', 'css', 'tag' or 'xpath'."
                )
            return element
        except Exception as e:
            logging.info(f"Element not found: {e}")
            raise e

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
        # create data download directory
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
        browserOpts = webdriver.ChromeOptions()
        configure_chrome_options(browserOpts, download_path)
        browser = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()), options=browserOpts
        )
        file_path = os.path.join(download_path, "reportTable.xlsx")
        if os.path.exists(file_path) and not is_valid_excel_download(file_path):
            os.remove(file_path)

        retries = 0
        while retries < self.max_retries:
            try:
                browser.get(
                    "https://vahan.parivahan.gov.in/vahan4dashboard/vahan/view/reportview.xhtml"
                )

                # select the state
                self.find_element(
                    browser,
                    "xpath",
                    '//label[starts-with(text(), "All Vahan4 Running States")]',
                ).click()
                time.sleep(2)

                self.find_element(
                    browser, "xpath", f'//li[starts-with(text(), "{state_label}")]'
                ).click()
                time.sleep(2)

                # selecting y_axis entering vehicle class as parameter
                self.find_element(browser, "id", "yaxisVar_label").click()
                time.sleep(2)
                self.find_element(browser, "id", "yaxisVar_1").click()
                time.sleep(2)

                # selecting x_axis entering fuel as parameter
                self.find_element(browser, "id", "xaxisVar_label").click()
                time.sleep(1)
                self.find_element(
                    browser, "xpath", "//ul[@id='xaxisVar_items']/li[text()='Fuel']"
                ).click()
                time.sleep(2)

                #  selecting year button and entering the value
                self.find_element(browser, "id", "selectedYear_label").click()
                time.sleep(2)
                self.find_element(
                    browser,
                    "xpath",
                    f"//ul[@id='selectedYear_items']/li[text()='{year_label}']",
                ).click()
                time.sleep(5)

                # click on main refresh button
                self.find_element(
                    browser,
                    "css",
                    "button[class='ui-button ui-widget ui-state-default ui-corner-all ui-button-text-icon-left button']",
                ).click()
                time.sleep(5)

                # click on month button
                self.find_element(
                    browser, "id", "groupingTable:selectMonth_label"
                ).click()
                time.sleep(2)
                # Enter month
                self.find_element(
                    browser,
                    "xpath",
                    f"//ul[@id='groupingTable:selectMonth_items']/li[text()='{month_label}']",
                ).click()
                time.sleep(2)

                # click on download button for downloading report
                self.find_element(browser, "id", "groupingTable:xls").click()
                wait_for_expected_download(download_path)
                browser.quit()
                logging.info(
                    f"file successfully downloaded for {state_folder_name}, {year_label}, {month_label}"
                )
                return
            except (
                    TimeoutException,
                    StaleElementReferenceException,
                    TimeoutError,
                    WebDriverException,
            ) as e:
                retries += 1
                if os.path.exists(file_path) and not is_valid_excel_download(file_path):
                    try:
                        os.remove(file_path)
                    except OSError:
                        pass
                logging.info(
                    f"Retrying attempt {retries} for {state_label}, {year_label}, {month_label}"
                )
                time.sleep(self.retry_delay)

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
        file_path = os.path.join(directory_path, "reportTable.xlsx")
        if not is_valid_excel_download(file_path):
            parameters.append((state, year, month))

    if not parameters:
        print(f"no missing files found  for {month} and {year}")
        return

    # Run selenium function in parallel
    with ThreadPoolExecutor(
            max_workers=10
    ) as executor:  # Adjust max_workers based on your system's capability
        futures = [
            executor.submit(data_extract_class.run_selenium, args)
            for args in parameters
        ]

        for future in as_completed(futures):
            try:
                result = future.result()
            except Exception as e:
                logging.info(f"Exception occurred: {e}")


if __name__ == "__main__":
    main()
