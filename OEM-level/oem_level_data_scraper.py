import shutil

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    StaleElementReferenceException,
    WebDriverException,
)
from datetime import datetime, timedelta
import os
import re
import time


class OEMDataScraper:
    def __init__(self):
        # create data download directory
        self.browserOpts = webdriver.ChromeOptions()

        self.browserOpts.browser_version = "stable"
        self.browserPrefs = {
            "credentials_enable_service": False,
            "profile.password_manager_enabled": False,
        }
        self.browserOpts.add_experimental_option(
            "excludeSwitches", ["enable-automation", "enable-logging"]
        )
        self.browserOpts.add_experimental_option("prefs", self.browserPrefs)
        self.browserOpts.add_argument("--disable-single-click-autofill")
        self.browserOpts.set_capability("goog:loggingPrefs", {"performance": "ALL"})
        self.browser = webdriver.Chrome(options=self.browserOpts)
        self.max_retries = 5
        self.retry_delay = 5

    @staticmethod
    def create_directory_if_not_exists(directory_path):
        """
        Create a new directory if it does not exist at a given location
        :param directory_path:  path to directory
        :return:
        """
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)
            print(f"Directory '{directory_path}' created.")
        else:
            print(f"Directory '{directory_path}' already exists.")

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
            print(f"Element not found: {e}")
            raise e

    @staticmethod
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

    def extract_oem_data_by_vehicle_category(
        self, year_label, month_label, vehicle_category_label
    ):
        """
        :param year_label: Year label for data
        :param month_label: Month label for data
        :param vehicle_category_label: vehicle category element
        :return: Downloads csv file in directory set up by chrome
        """
        # create data download directory
        vehicle_category_folder_name = re.sub(
            r"\W+", " ", vehicle_category_label
        ).rstrip()
        download_path = os.path.join(
            os.getcwd(),
            "OEM-level",
            "oem_data_by_category",
            vehicle_category_folder_name.rstrip(),
            str(year_label),
            month_label,
        )
        self.create_directory_if_not_exists(download_path)
        self.browserPrefs.update({"download.default_directory": download_path})
        self.browserOpts.add_experimental_option("prefs", self.browserPrefs)
        self.browser = webdriver.Chrome(options=self.browserOpts)
        retries = 0
        while retries < self.max_retries:
            try:
                self.browser.get(
                    "https://vahan.parivahan.gov.in/vahan4dashboard/vahan/view/reportview.xhtml"
                )

                # selecting y_axis entering maker as parameter
                self.find_element(self.browser, "id", "yaxisVar_label").click()
                time.sleep(1)
                self.find_element(self.browser, "id", "yaxisVar_4").click()
                time.sleep(2)

                # selecting x_axis entering fuel as parameter
                self.find_element(self.browser, "id", "xaxisVar_label").click()
                time.sleep(1)
                self.find_element(self.browser, "id", "xaxisVar_2").click()
                time.sleep(2)

                #  selecting year button and entering the value
                self.find_element(self.browser, "id", "selectedYear_label").click()
                time.sleep(1)
                self.find_element(
                    self.browser,
                    "xpath",
                    f"//ul[@id='selectedYear_items']/li[text()='{year_label}']",
                ).click()
                time.sleep(5)

                # click on main refresh button
                self.find_element(
                    self.browser,
                    "css",
                    "button[class='ui-button ui-widget ui-state-default ui-corner-all ui-button-text-icon-left button']",
                ).click()
                time.sleep(5)

                # click on month button
                self.find_element(
                    self.browser, "id", "groupingTable:selectMonth"
                ).click()
                time.sleep(1)
                # Enter month
                self.find_element(
                    self.browser,
                    "xpath",
                    f"//ul[@id='groupingTable:selectMonth_items']/li[text()='{month_label}']",
                ).click()
                time.sleep(2)

                # click on span toggler on left
                self.find_element(self.browser, "id", "filterLayout-toggler").click()
                time.sleep(1)

                # selecting adapted vehicle as category
                self.find_element(
                    self.browser, "xpath", f"//label[text()='{vehicle_category_label}']"
                ).click()
                time.sleep(5)

                # click on refresh button on left toggler object
                self.find_element(
                    self.browser,
                    "css",
                    "button[class='ui-button ui-widget ui-state-default ui-corner-all ui-button-text-icon-left']",
                ).click()
                time.sleep(5)

                # click on download button for downloading report
                self.find_element(self.browser, "id", "groupingTable:xls").click()
                time.sleep(5)
                self.browser.quit()
                break

            except (
                TimeoutException,
                StaleElementReferenceException,
                WebDriverException,
            ) as e:
                retries += 1
                print(
                    f"Retrying attempt {retries} for {year_label}, {month_label} and {vehicle_category_label}"
                )
                time.sleep(self.retry_delay)

    def get_all_vehicle_category_elements(self):
        retries = 0
        while retries < self.max_retries:
            try:
                self.browser.get(
                    "https://vahan.parivahan.gov.in/vahan4dashboard/vahan/view/reportview.xhtml"
                )
                # click on span toggler on left
                self.find_element(self.browser, "id", "filterLayout-toggler").click()
                # find vehicle category element
                category_element = self.find_element(
                    self.browser, "xpath", '//table[@id="VhClass"]/tbody'
                )
                time.sleep(2)
                vehicle_category_lst = []
                for row_label in category_element.find_elements(By.TAG_NAME, "tr"):
                    vehicle_category_lst.append(row_label.text)
                return vehicle_category_lst

            except (
                TimeoutException,
                StaleElementReferenceException,
                WebDriverException,
            ) as e:
                print(f"Vehicle Category element function threw exception {e}")
                retries += 1
                print(f"Retrying attempt {retries} for vehicle category label")


def main():
    # for back-fill of data till 2023
    data_extract_class = OEMDataScraper()

    # get all vehicle categories
    vehicle_category_lst = data_extract_class.get_all_vehicle_category_elements()

    # get month abbreviation and year
    # month, year = data_extract_class.get_year_month_label()
    month, year = "JAN", 2024

    for category in vehicle_category_lst:
        directory_path = os.path.join(
            os.getcwd(),
            "OEM-level",
            "oem_data_by_category",
            re.sub(r"\W+", " ", category).rstrip(),
            str(year),
            month
        )

        # remove the file if already exists from previous month
        if os.path.exists(directory_path):
            shutil.rmtree(directory_path)

        data_extract_class.extract_oem_data_by_vehicle_category(year, month, category)

        # check if file was downloaded
        if not os.path.exists(os.path.join(directory_path, "reportTable.xlsx")):
            print(f"file not downloaded for {category}, {year}, {month}")

    # Used for one time extract
    # TODO: Remove post review

    # years = [2024 - i for i in range(1)]
    # months = [
    #     "JAN",
    #     "FEB",
    #     "MAR",
    #     "APR",
    #     "MAY",
    #     "JUN",
    #     "JUL",
    #     "AUG",
    #     "SEP",
    #     "OCT",
    #     "NOV",
    #     "DEC",
    # ]
    # for year in years:
    #     for month in months:
    #         for vehicle_category in vehicle_category_lst:
    #             data_extract_class.extract_oem_data_by_vehicle_category(
    #                 year, month, vehicle_category
    #             )

    # delete path of file if the file does not exist
    # file_missing_lst = []
    # for vehicle_category in os.listdir(parent_directory):
    #
    #     vehicle_category_path = os.path.join(parent_directory, vehicle_category)
    #
    #     # Check if it's a directory
    #     if os.path.isdir(vehicle_category_path):
    #         for year in os.listdir(vehicle_category_path):
    #             year_path = os.path.join(vehicle_category_path, year)
    #
    #             for month in os.listdir(year_path):
    #                 month_path = os.path.join(year_path, month)
    #                 file_path = os.path.join(month_path, "reportTable.xlsx")
    #                 if not os.path.exists(file_path):
    #                     print(f"file not found for {vehicle_category}, {year}, {month}")

    #                     file_missing_lst.append((vehicle_category, year, month))
    #                     shutil.rmtree(month_path)
    # return file_missing_lst

    # file_missing_lst = data_extract_class.file_download_test(parent_directory)

    # print(len(vehicle_category_lst))
    # years = [2024]
    #
    # parent_directory = os.path.join(os.getcwd(), "OEM-level", "oem_data_by_category")
    #
    # # get missing file combinations
    # parameter_combinations = [
    #     (category, year, month)
    #     for category in vehicle_category_lst
    #     for year in years
    #     for month in months
    #     if not os.path.exists(os.path.join(parent_directory, re.sub(
    #         r"\W+", " ", category
    #     ).rstrip(), str(year), month, "reportTable.xlsx"))
    # ]
    #
    # for combination in parameter_combinations:
    #     vehicle_category_label = combination[0]
    #     year_label = combination[1]
    #     month_label = combination[2]
    #     while not os.path.exists(os.path.join(parent_directory, re.sub(
    #             r"\W+", " ", vehicle_category_label
    #     ).rstrip(), str(year_label), month_label, "reportTable.xlsx")):
    #         data_extract_class.extract_oem_data_by_vehicle_category(year_label, month_label, vehicle_category_label)


if __name__ == "__main__":
    main()
