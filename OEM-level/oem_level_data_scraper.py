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
        self.browserOpts.add_argument("--headless")
        self.browserOpts.add_argument("--no-sandbox")
        self.browserOpts.add_argument("--disable-dev-shm-usage")
        self.browserOpts.add_argument("--disable-single-click-autofill")
        self.browserOpts.set_capability("goog:loggingPrefs", {"performance": "ALL"})
        self.browser = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=self.browserOpts)
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
            pass

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

    def extract_oem_data_by_state_and_vehicle_category(
            self, state_label, year_label, month_label, vehicle_category_label
    ):
        """
        :param state_label: State label for data
        :param year_label: Year label for data
        :param month_label: Month label for data
        :param vehicle_category_label: vehicle category element
        :return: Downloads csv file in directory set up by chrome
        """
        # create data download directory
        state_folder_name = re.sub(r"[^a-zA-Z\s]", " ", state_label).rstrip()
        vehicle_category_folder_name = re.sub(
            r"\W+", " ", vehicle_category_label
        ).rstrip()
        download_path = os.path.join(
            os.getcwd(),
            "OEM-level",
            "oem_data_by_state_and_category",
            state_folder_name.rstrip(),
            vehicle_category_folder_name.rstrip(),
            str(year_label),
            month_label,
        )
        self.create_directory_if_not_exists(download_path)
        self.browserPrefs.update({"download.default_directory": download_path})
        self.browserOpts.add_experimental_option("prefs", self.browserPrefs)
        self.browser = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=self.browserOpts)
        retries = 0
        while retries < self.max_retries:
            try:
                self.browser.get(
                    "https://vahan.parivahan.gov.in/vahan4dashboard/vahan/view/reportview.xhtml"
                )

                # select the state
                self.find_element(
                    self.browser,
                    "xpath",
                    '//label[starts-with(text(), "All Vahan4 Running States")]',
                ).click()
                time.sleep(2)

                self.find_element(
                    self.browser, "xpath", f'//li[starts-with(text(), "{state_label}")]'
                ).click()
                time.sleep(2)

                # selecting y_axis entering maker as parameter
                self.find_element(self.browser, "id", "yaxisVar_label").click()
                time.sleep(1)
                self.find_element(self.browser, "id", "yaxisVar_4").click()
                time.sleep(2)

                # selecting x_axis entering fuel as parameter
                self.find_element(self.browser, "id", "xaxisVar_label").click()
                time.sleep(1)
                self.find_element(self.browser, "xpath", "//ul[@id='xaxisVar_items']/li[text()='Fuel']").click()
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
                    self.browser, "id", "groupingTable:selectMonth_label"
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
                    f"Retrying attempt {retries} for {state_label}, {year_label}, {month_label} and {vehicle_category_label}"
                )
                time.sleep(self.retry_delay)

    def get_all_vehicle_category_elements(self):
        retries = 0
        while retries < self.max_retries:
            try:
                self.browser.get(
                    "https://vahan.parivahan.gov.in/vahan4dashboard/vahan/view/reportview.xhtml"
                )
                time.sleep(2)
                # click on span toggler on left
                self.find_element(self.browser, "id", "filterLayout-toggler").click()
                time.sleep(2)
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
    data_extract_class = OEMDataScraper()

    # get all state and vehicle category elements
    vehicle_category_lst = data_extract_class.get_all_vehicle_category_elements()

    state_lst = [
        "Andaman & Nicobar Island",
        "Andhra Pradesh",
        "Arunachal Pradesh",
        "Assam",
        "Bihar",
        "Chhattisgarh",
        "Chandigarh",
        "UT of DNH and DD",
        "Delhi",
        "Goa",
        "Gujarat",
        "Himachal Pradesh",
        "Haryana",
        "Jharkhand",
        "Jammu and Kashmir",
        "Karnataka",
        "Kerala",
        "Ladakh",
        "Maharashtra",
        "Meghalaya",
        "Manipur",
        "Madhya Pradesh",
        "Mizoram",
        "Nagaland",
        "Odisha",
        "Punjab",
        "Puducherry",
        "Rajasthan",
        "Sikkim",
        "Tamil Nadu",
        "Tripura",
        "Uttarakhand",
        "Uttar Pradesh",
        "West Bengal",
    ]

    month, year = data_extract_class.get_year_month_label()

    for state in state_lst:
        for category in vehicle_category_lst:
            directory_path = os.path.join(
                os.getcwd(),
                "OEM-level",
                "oem_data_by_state_and_category",
                re.sub(r"\W+", " ", category).rstrip(),
                state,
                str(year),
                month
            )

            # remove the file if already exists from previous month
            if os.path.exists(directory_path):
                shutil.rmtree(directory_path)

            data_extract_class.extract_oem_data_by_state_and_vehicle_category(state, year, month, category)

            # check if file was downloaded
            if not os.path.exists(os.path.join(directory_path, "reportTable.xlsx")):
                print(f"file not downloaded for {state}, {category}, {year}, {month}")


if __name__ == "__main__":
    main()
