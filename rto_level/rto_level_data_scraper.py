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

from pipeline_logging import configure_pipeline_logging
from utils import *

configure_pipeline_logging()


def merge_state_rto_mappings(previous_mapping, fresh_mapping):
    """Prefer newly fetched RTO lists, but keep the last known list as fallback."""
    merged_mapping = dict(previous_mapping or {})
    for state, offices in (fresh_mapping or {}).items():
        if offices:
            merged_mapping[state] = offices
    return merged_mapping


def get_missing_mapping_states(state_rto_mapping, states):
    """Return states that still do not have any mapped RTO offices."""
    return [state for state in states if not state_rto_mapping.get(state)]


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
        return re.sub(r"[\\/]", "", name).strip()

    @staticmethod
    def build_rto_folder_name(rto_label):
        rto_name_code = RTODataScraper.extract_rto_name_and_code(rto_label)
        if not rto_name_code:
            return None
        return RTODataScraper.sanitize_folder_name(rto_name_code)

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
        rto_folder_name = self.build_rto_folder_name(rto_label)
        if not rto_folder_name:
            raise ValueError(f"Unable to parse RTO label: {rto_label}")
        rto_office_code = rto_folder_name.rsplit("_", 1)[1]

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
                    "Downloaded RTO report for state=%s rto=%s year=%s month=%s",
                    state_folder_name,
                    rto_folder_name,
                    year_label,
                    month_label,
                )
                return
            except (
                TimeoutException,
                StaleElementReferenceException,
                WebDriverException,
            ) as e:
                retries += 1
                logging.info(
                    "Retrying RTO download attempt %s for state=%s rto=%s year=%s month=%s",
                    retries,
                    state_label,
                    rto_label,
                    year_label,
                    month_label,
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
            for future in as_completed(futures):
                state = futures[future]
                try:
                    result = future.result()  # Wait for the thread to finish
                    if result:
                        results.update(result)  # Add the state's result to the dictionary
                    else:
                        logging.warning(
                            "No RTO offices were fetched for state '%s'.",
                            state,
                        )
                except Exception as e:
                    logging.warning(f"Error fetching offices for {state}: {e}")
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
    previous_mapping = data_extract_class.load_previous_mapping()

    try:
        fresh_mapping = data_extract_class.run_for_all_states(state_lst)
    except Exception as e:
        fresh_mapping = {}
        logging.error(
            "RTO fetching failed with exception: %s. Falling back to older output.json if available.",
            e,
        )

    state_rto_mapping = merge_state_rto_mappings(previous_mapping, fresh_mapping)

    fresh_missing_states = get_missing_mapping_states(fresh_mapping, state_lst)
    merged_missing_states = get_missing_mapping_states(state_rto_mapping, state_lst)

    if fresh_mapping:
        logging.info(
            "Fetched live RTO mapping for %s out of %s states.",
            len(fresh_mapping),
            len(state_lst),
        )
        if fresh_missing_states:
            logging.warning(
                "Live RTO mapping refresh missed states and will fall back to the previous output.json for: %s",
                ", ".join(fresh_missing_states),
            )
        with open("output.json", "w") as rto_mapping_output:
            json.dump(state_rto_mapping, rto_mapping_output, indent=4)
        logging.info(
            "Saved merged RTO mapping to output.json with coverage for %s states.",
            len(state_lst) - len(merged_missing_states),
        )
    elif previous_mapping:
        logging.warning(
            "Live RTO mapping refresh returned no states. Continuing with the previous output.json."
        )
    else:
        logging.error(
            "Live RTO mapping refresh returned no states and no previous output.json is available."
        )

    if merged_missing_states:
        raise RuntimeError(
            "RTO mapping is still missing states after refresh: "
            + ", ".join(merged_missing_states)
        )

    if len(sys.argv) > 2:
        # if month and year are passed as command-line arguments
        month = sys.argv[1]
        year = sys.argv[2]
    else:
        # use the default function to get the month and year
        month, year = get_year_month_label()

    parameters = []
    invalid_rto_labels = []
    for state in state_lst:
        # get all RTO office names for state
        all_rto_office_names = state_rto_mapping.get(state, [])
        for rto_office_name in all_rto_office_names:
            rto_folder_name = data_extract_class.build_rto_folder_name(rto_office_name)
            if not rto_folder_name:
                invalid_rto_labels.append((state, rto_office_name))
                continue
            directory_path = os.path.join(
                os.getcwd(),
                "rto_level",
                "rto_level_ev_data",
                re.sub(r"[^a-zA-Z\s]", " ", state).rstrip(),
                rto_folder_name,
                str(year),
                month,
            )
            # remove the file if already exists from previous month
            if os.path.exists(directory_path):
                shutil.rmtree(directory_path)

            parameters.append((state, rto_office_name, year, month))

    if invalid_rto_labels:
        sample_state, sample_label = invalid_rto_labels[0]
        logging.error(
            "Skipped %s RTO labels that could not be converted into folder names. Example: state=%s label=%s",
            len(invalid_rto_labels),
            sample_state,
            sample_label,
        )

    logging.info(
        "Prepared %s RTO download tasks for %s %s.",
        len(parameters),
        month,
        year,
    )

    # run selenium function in parallel
    with ThreadPoolExecutor(
        max_workers=35
    ) as executor:  # adjust max_workers based on your system's capability
        futures = {
            executor.submit(data_extract_class.run_selenium, args): args
            for args in parameters
        }
        failed_downloads = []
        successful_downloads = 0

        for future in as_completed(futures):
            state_label, rto_label, year_label, month_label = futures[future]
            try:
                future.result()
                successful_downloads += 1
            except Exception as e:
                failed_downloads.append((state_label, rto_label))
                logging.warning(
                    "RTO download failed for state=%s rto=%s year=%s month=%s: %s",
                    state_label,
                    rto_label,
                    year_label,
                    month_label,
                    e,
                )

    logging.info(
        "RTO extraction summary for %s %s: prepared=%s succeeded=%s failed=%s",
        month,
        year,
        len(parameters),
        successful_downloads,
        len(failed_downloads),
    )


if __name__ == "__main__":
    main()
