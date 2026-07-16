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
        configure_chrome_options(browser_options)
        browser = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()), options=browser_options
        )
        context = {
            "pipeline": "rto",
            "state": state,
            "action": "refresh_rto_mapping",
        }

        try:
            all_rto_office_names = []
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
                    time.sleep(5)

                    find_element(
                        browser,
                        "xpath",
                        '//label[starts-with(text(), "All Vahan4 Running States")]',
                        step="open_state_dropdown",
                        context=context,
                    ).click()
                    time.sleep(2)

                    find_element(
                        browser,
                        "xpath",
                        f'//li[starts-with(text(), "{state}")]',
                        step="select_state",
                        context=context,
                    ).click()
                    time.sleep(2)

                    find_element(
                        browser,
                        "xpath",
                        '//label[starts-with(text(), "All Vahan4 Running Office")]',
                        step="open_rto_dropdown",
                        context=context,
                    ).click()
                    time.sleep(2)

                    rto_list = find_element(
                        browser,
                        "xpath",
                        "//ul[@id='selectedRto_items']",
                        step="read_rto_list",
                        context=context,
                    )
                    for elem in rto_list.find_elements(By.TAG_NAME, "li"):
                        if "All Vahan4 Running Office" not in elem.text:
                            all_rto_office_names.append(elem.text)

                    logging.info(
                        "Fetched RTO mapping state=%s offices=%s",
                        state,
                        len(all_rto_office_names),
                    )
                    return {state: all_rto_office_names}

                except BlockedPageError as e:
                    logging.error(
                        "RTO mapping refresh blocked context=%s page_title=%s diagnostics=%s error=%s",
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
                        "Retrying RTO mapping fetch attempt=%s/%s context=%s failed_step=%s error=%s",
                        retries,
                        self.max_retries,
                        format_log_context(context),
                        getattr(e, "step", "refresh_rto_mapping"),
                        summarize_exception(e),
                    )
                    time.sleep(self.retry_delay)

            raise last_exception
        finally:
            browser.quit()

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

        create_directory_if_not_exists(download_path)

        browserOpts = webdriver.ChromeOptions()
        configure_chrome_options(browserOpts, download_path)

        browser = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()), options=browserOpts
        )
        context = {
            "pipeline": "rto",
            "state": state_label,
            "rto_label": rto_label,
            "rto_code": rto_office_code,
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

                    find_element(
                        browser,
                        "xpath",
                        '//label[starts-with(text(), "All Vahan4 Running States")]',
                        step="open_state_dropdown",
                        context=context,
                    ).click()
                    time.sleep(2)

                    find_element(
                        browser,
                        "xpath",
                        f'//li[starts-with(text(), "{state_label}")]',
                        step="select_state",
                        context=context,
                    ).click()
                    time.sleep(2)

                    find_element(
                        browser,
                        "xpath",
                        '//label[starts-with(text(), "All Vahan4 Running Office")]',
                        step="open_rto_dropdown",
                        context=context,
                    ).click()
                    time.sleep(1)

                    find_element(
                        browser,
                        "xpath",
                        f'//ul[@id="selectedRto_items"]/li[contains(text(), "{rto_office_code}")]',
                        step="select_rto_office",
                        context=context,
                    ).click()
                    time.sleep(2)

                    find_element(
                        browser,
                        "id",
                        "yaxisVar_label",
                        step="open_y_axis_dropdown",
                        context=context,
                    ).click()
                    time.sleep(2)
                    find_element(
                        browser,
                        "id",
                        "yaxisVar_1",
                        step="select_y_axis_vehicle_class",
                        context=context,
                    ).click()
                    time.sleep(2)

                    find_element(
                        browser,
                        "id",
                        "xaxisVar_label",
                        step="open_x_axis_dropdown",
                        context=context,
                    ).click()
                    time.sleep(1)
                    find_element(
                        browser,
                        "xpath",
                        "//ul[@id='xaxisVar_items']/li[text()='Fuel']",
                        step="select_x_axis_fuel",
                        context=context,
                    ).click()
                    time.sleep(2)

                    find_element(
                        browser,
                        "id",
                        "selectedYear_label",
                        step="open_year_dropdown",
                        context=context,
                    ).click()
                    time.sleep(2)
                    find_element(
                        browser,
                        "xpath",
                        f"//ul[@id='selectedYear_items']/li[text()='{year_label}']",
                        step="select_year",
                        context=context,
                    ).click()
                    time.sleep(5)

                    find_element(
                        browser,
                        "css",
                        "button[class='ui-button ui-widget ui-state-default ui-corner-all ui-button-text-icon-left button']",
                        step="click_main_refresh",
                        context=context,
                    ).click()
                    time.sleep(5)

                    find_element(
                        browser,
                        "id",
                        "groupingTable:selectMonth_label",
                        step="open_month_dropdown",
                        context=context,
                    ).click()
                    time.sleep(2)
                    find_element(
                        browser,
                        "xpath",
                        f"//ul[@id='groupingTable:selectMonth_items']/li[text()='{month_label}']",
                        step="select_month",
                        context=context,
                    ).click()
                    time.sleep(2)

                    find_element(
                        browser,
                        "id",
                        "groupingTable:xls",
                        step="download_report",
                        context=context,
                    ).click()
                    time.sleep(5)
                    logging.info(
                        "Downloaded RTO report state=%s rto=%s year=%s month=%s",
                        state_folder_name,
                        rto_folder_name,
                        year_label,
                        month_label,
                    )
                    return
                except BlockedPageError as e:
                    logging.error(
                        "RTO page access blocked context=%s page_title=%s diagnostics=%s error=%s",
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
                        "Retrying RTO download attempt=%s/%s context=%s failed_step=%s error=%s",
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
                    logging.warning(
                        "RTO mapping refresh failed state=%s failed_step=%s diagnostics=%s error=%s",
                        state,
                        getattr(e, "step", "refresh_rto_mapping"),
                        getattr(e, "diagnostics", {}).get("metadata_path", ""),
                        summarize_exception(getattr(e, "original_exception", e)),
                    )
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
                logging.error(
                    "RTO download failed state=%s rto=%s year=%s month=%s failed_step=%s diagnostics=%s error=%s",
                    state_label,
                    rto_label,
                    year_label,
                    month_label,
                    getattr(e, "step", "download_flow"),
                    getattr(e, "diagnostics", {}).get("metadata_path", ""),
                    summarize_exception(getattr(e, "original_exception", e)),
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
