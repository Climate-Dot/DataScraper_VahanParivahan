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
    def sanitize_folder_name(name):
        """replace or remove special characters to make a valid folder name."""
        return re.sub(r"[\/]", "", name).strip()

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
        browserOpts.add_experimental_option(
            "excludeSwitches", ["enable-automation", "enable-logging"]
        )
        browserOpts.add_argument("--headless")
        browserOpts.add_argument("--no-sandbox")
        browserOpts.add_argument("--disable-dev-shm-usage")
        browserOpts.add_argument("--disable-single-click-autofill")
        browserOpts.set_capability("goog:loggingPrefs", {"performance": "ALL"})

        # create data download directory
        state_folder_name = re.sub(r"[^a-zA-Z\s]", " ", state_label).rstrip()
        rto_name_code = self.extract_rto_name_and_code(rto_label)
        rto_folder_name = self.sanitize_folder_name(rto_name_code)
        rto_office_code = rto_folder_name.split("_")[1]

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
                time.sleep(2)

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


def main():
    data_extract_class = RTODataScraper()

    with open("output.json", "r") as f:
        state_rto_mapping = json.load(f)

    years = [2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]
    parameters = []
    for year in years:
        for month in month_mapping.keys():
            for state in state_lst:
                # get all RTO office names for state
                all_rto_office_names = state_rto_mapping.get(state)
                for rto_office_name in all_rto_office_names:
                    rto_office_name_code = data_extract_class.extract_rto_name_and_code(
                        rto_office_name
                    )
                    rto_folder_name = data_extract_class.sanitize_folder_name(
                        rto_office_name_code
                    )
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
                    if not os.path.exists(
                        os.path.join(directory_path, "reportTable.xlsx")
                    ):
                        parameters.append((state, rto_office_name, year, month))
    print(parameters)
    print(len(parameters))

    # run selenium function in parallel
    # with ThreadPoolExecutor(
    #     max_workers=50
    # ) as executor:  # adjust max_workers based on your system's capability
    #     futures = [
    #         executor.submit(data_extract_class.run_selenium, args)
    #         for args in parameters
    #     ]
    #
    #     for future in as_completed(futures):
    #         try:
    #             result = future.result()
    #         except Exception as e:
    #             logging.info(f"Exception occurred: {e}")


if __name__ == "__main__":
    main()
