from concurrent.futures import ThreadPoolExecutor, as_completed

from selenium import webdriver
from selenium.common.exceptions import (
    TimeoutException,
    StaleElementReferenceException,
    WebDriverException,
)
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service

import json
import logging
import os
import sys
import shutil
import time

# configure logging to write to both the console and a file
logging.basicConfig(
    level=logging.INFO,  # Set the logging level (INFO, DEBUG, etc.)
    format="%(asctime)s - %(levelname)s - %(message)s",  # log message format
)

repo_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if repo_path not in sys.path:
    sys.path.append(repo_path)

from utils import *


class RTODataScraper:
    def __init__(self):
        self.max_retries = 5
        self.retry_delay = 15

    @staticmethod
    def extract_rto_name_and_code(rto_label):
        # extract the region and state code using a regular expression
        match = re.match(r"(.*) - ([A-Z0-9]+)\(", rto_label)
        if match:
            region = match.group(1).strip()
            state_code = match.group(2).strip()
            # Combine them into the desired format
            return f"{region}_{state_code}"
        return None  # return None if the format doesn't match

    @staticmethod
    def sanitize_folder_name(name):
        """replace or remove special characters to make a valid folder name."""
        return re.sub(r"[\/]", "", name).strip()

    def get_all_rto_from_state(self, state):
        """
        Function is used to get all the rto from the given state
        :param state: string
        :return: list of all rto from the state
        """

        browser_options = webdriver.ChromeOptions()
        browser_options.browser_version = "stable"
        browserPrefs = {
            "credentials_enable_service": False,
            "profile.password_manager_enabled": False,
        }
        browser_options.add_experimental_option("prefs", browserPrefs)
        browser_options.add_argument("--headless")
        browser = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()), options=browser_options
        )

        all_rto_office_names = []
        retries = 0
        while retries < self.max_retries:
            try:
                browser.get(
                    "https://vahan.parivahan.gov.in/vahan4dashboard/vahan/view/reportview.xhtml"
                )
                time.sleep(5)

                # click on state drop down
                find_element(
                    browser,
                    "xpath",
                    '//label[starts-with(text(), "All Vahan4 Running States")]',
                ).click()
                time.sleep(2)

                # click on state label
                find_element(
                    browser, "xpath", f'//li[starts-with(text(), "{state}")]'
                ).click()
                time.sleep(2)

                # click on the rto drop down
                find_element(
                    browser,
                    "xpath",
                    '//label[starts-with(text(), "All Vahan4 Running Office")]',
                ).click()
                time.sleep(2)

                # get list of rtos
                rto_list = find_element(
                    browser,
                    "xpath",
                    "//ul[@id='selectedRto_items']",
                )
                for elem in rto_list.find_elements(By.TAG_NAME, "li"):
                    if "All Vahan4 Running Office" not in elem.text:
                        all_rto_office_names.append(elem.text)
                return {state: all_rto_office_names}

            except (
                TimeoutException,
                StaleElementReferenceException,
                WebDriverException,
            ) as e:
                logging.warning(f"rto category element function threw exception {e}")
                retries += 1
                logging.warning(f"retrying attempt {retries} for rto label")

    def extract_rto_level_data(
        self,
        state_label,
        rto_label,
        year_label,
        month_label,
    ):
        """
        :param state_label: State label for data
        :param rto_label: vehicle category element
        :param year_label: Year label for data
        :param month_label: Month label for data
        :return downloads csv file in directory set up by chrome
        """
        # create data download directory
        browserOpts = webdriver.ChromeOptions()
        browserOpts.browser_version = "stable"
        browserPrefs = {
            "credentials_enable_service": False,
            "profile.password_manager_enabled": False,
        }
        browserOpts.add_argument("--headless")

        # create data download directory
        state_folder_name = re.sub(r"[^a-zA-Z\s]", " ", state_label).rstrip()
        rto_name_code = self.extract_rto_name_and_code(rto_label)
        rto_folder_name = self.sanitize_folder_name(rto_name_code)
        rto_office_code = rto_folder_name.split("_")[1]
        rto_folder_name = self.extract_rto_name_and_code(rto_label)

        download_path = os.path.join(
            os.getcwd(),
            "rto_level",
            "rto_level_ev_data",
            state_folder_name.rstrip(),
            rto_folder_name,
            str(year_label),
            month_label,
        )

        browserPrefs.update({"download.default_directory": download_path})
        browserOpts.add_experimental_option("prefs", browserPrefs)

        create_directory_if_not_exists(download_path)

        browser = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()), options=browserOpts
        )
        retries = 0
        while retries < self.max_retries:
            try:
                browser.get(
                    "https://vahan.parivahan.gov.in/vahan4dashboard/vahan/view/reportview.xhtml"
                )

                # select the state
                find_element(
                    browser,
                    "xpath",
                    '//label[starts-with(text(), "All Vahan4 Running States")]',
                ).click()
                time.sleep(2)

                find_element(
                    browser, "xpath", f'//li[starts-with(text(), "{state_label}")]'
                ).click()
                time.sleep(2)

                find_element(
                    browser,
                    "xpath",
                    '//label[starts-with(text(), "All Vahan4 Running Office")]',
                ).click()
                time.sleep(1)

                find_element(
                    browser,
                    "xpath",
                    f'//ul[@id="selectedRto_items"]/li[contains(text(), "{rto_office_code}")]',
                ).click()
                time.sleep(2)

                # selecting y_axis entering vehicle class as parameter
                find_element(browser, "id", "yaxisVar_label").click()
                time.sleep(2)
                find_element(browser, "id", "yaxisVar_1").click()
                time.sleep(2)

                # selecting x_axis entering fuel as parameter
                find_element(browser, "id", "xaxisVar_label").click()
                time.sleep(1)
                find_element(
                    browser, "xpath", "//ul[@id='xaxisVar_items']/li[text()='Fuel']"
                ).click()
                time.sleep(2)

                #  selecting year button and entering the value
                find_element(browser, "id", "selectedYear_label").click()
                time.sleep(2)
                find_element(
                    browser,
                    "xpath",
                    f"//ul[@id='selectedYear_items']/li[text()='{year_label}']",
                ).click()
                time.sleep(5)

                # click on main refresh button
                find_element(
                    browser,
                    "css",
                    "button[class='ui-button ui-widget ui-state-default ui-corner-all ui-button-text-icon-left button']",
                ).click()
                time.sleep(5)

                # click on month button
                find_element(browser, "id", "groupingTable:selectMonth_label").click()
                time.sleep(2)
                # Enter month
                find_element(
                    browser,
                    "xpath",
                    f"//ul[@id='groupingTable:selectMonth_items']/li[text()='{month_label}']",
                ).click()
                time.sleep(2)

                # click on download button for downloading report
                find_element(browser, "id", "groupingTable:xls").click()
                time.sleep(5)
                browser.quit()
                logging.info(
                    f"file successfully downloaded for {state_folder_name}, {rto_folder_name}, {year_label}, {month_label}"
                )
                return
            except (
                TimeoutException,
                StaleElementReferenceException,
                WebDriverException,
            ) as e:
                retries += 1
                logging.info(
                    f"retrying attempt {retries} for {state_label}, {rto_label}, {year_label},{month_label}"
                )
                time.sleep(self.retry_delay)

    # define a function to wrap the selenium function for argument unpacking
    def run_selenium(self, args):
        return self.extract_rto_level_data(*args)

    def run_for_all_states(self, states):
        results = {}
        with ThreadPoolExecutor(
            max_workers=34
        ) as executor:  # Adjust max_workers as needed
            futures = {
                executor.submit(self.get_all_rto_from_state, state): state
                for state in states
            }
            for future in futures:
                state = futures[future]
                try:
                    result = future.result()  # Wait for the thread to finish
                    results.update(result)  # Add the state's result to the dictionary
                except Exception as e:
                    print(f"Error fetching offices for {state}: {e}")
        return results

    @staticmethod
    def load_previous_mapping():
        """
        function is used to load previous version of output.json if the current fetch fails due to any reason
        :return:
        """
        if os.path.exists("output.json"):
            try:
                with open("output.json", "r") as f:
                    return json.load(f)
            except json.JSONDecodeError:
                logging.error("Previous output.json is corrupted. Starting fresh.")
        return {}


def main():
    data_extract_class = RTODataScraper()

    try:
        previous_mapping = data_extract_class.load_previous_mapping()
        state_rto_mapping = previous_mapping

        # Check if all states returned results
        if len(state_rto_mapping) < len(state_lst):
            logging.warning(
                f"Only fetched {len(state_rto_mapping)} out of {len(state_lst)} states. Merging with previous data."
            )

        # Save updated mapping only if it contains some valid data
        if state_rto_mapping and len(state_rto_mapping) == len(state_lst):
            logging.info(
                f"rto fetching successful. fetched for {len(state_rto_mapping)} states.. saving the mapping to file"
            )
            with open("output.json", "w") as rto_mapping_output:
                json.dump(state_rto_mapping, rto_mapping_output, indent=4)
        else:
            logging.error("No valid data fetched. Using only previous mapping.")

    except Exception as e:
        state_rto_mapping = previous_mapping
        logging.error(
            f"RTO fetching failed with exception: {e}. Using older output.json file if available."
        )

    if len(sys.argv) > 2:
        # if month and year are passed as command-line arguments
        month = sys.argv[1]
        year = sys.argv[2]
    else:
        # use the default function to get the month and year
        month, year = get_year_month_label()

    parameters = []
    for state in state_lst:
        # get all RTO office names for state
        all_rto_office_names = state_rto_mapping.get(state)
        for rto_office_name in all_rto_office_names:
            directory_path = os.path.join(
                os.getcwd(),
                "rto_level",
                "rto_level_ev_data",
                re.sub(r"[^a-zA-Z\s]", " ", state).rstrip(),
                data_extract_class.extract_rto_name_and_code(rto_office_name),
                str(year),
                month,
            )
            # remove the file if already exists from previous month
            if os.path.exists(directory_path):
                shutil.rmtree(directory_path)

            parameters.append((state, rto_office_name, year, month))

    # run selenium function in parallel
    with ThreadPoolExecutor(
        max_workers=35
    ) as executor:  # adjust max_workers based on your system's capability
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
