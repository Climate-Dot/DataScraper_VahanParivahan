from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import os
import time


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
        else:
            raise ValueError("Invalid identifier. Use 'id', 'css', or 'xpath'.")
        return element
    except Exception as e:
        print(f"Element not found: {e}")
        raise e


def create_directory_if_not_exists(directory_path):
    """
    Creates a directory if not exists at a given path
    :param directory_path: directory path
    """
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        print(f"Directory '{directory_path}' created.")
    else:
        print(f"Directory '{directory_path}' already exists.")


def extract_oem_data_by_vehicle_category(
        year_label, month_label, vehicle_category_label
):
    """
    :param year_label: Year label for data
    :param month_label: Month label for data
    :param vehicle_category_label: veg
    :return: Downloads csv file in directory set up by chrome
    """
    # create data download directory
    download_path = os.path.join(
        os.getcwd(), "OEM-level", "oem_data_by_category", vehicle_category_label, str(year_label), month_label
    )
    create_directory_if_not_exists(download_path)
    browserOpts = webdriver.ChromeOptions()

    browserOpts.browser_version = "stable"
    browserPrefs = {
        "credentials_enable_service": False,
        "profile.password_manager_enabled": False,
        "download.default_directory": download_path,
    }
    browserOpts.add_experimental_option(
        "excludeSwitches", ["enable-automation", "enable-logging"]
    )
    browserOpts.add_experimental_option("prefs", browserPrefs)
    browserOpts.add_argument("--disable-single-click-autofill")
    browserOpts.set_capability("goog:loggingPrefs", {"performance": "ALL"})
    browser = webdriver.Chrome(options=browserOpts)
    try:
        browser.get(
            "https://vahan.parivahan.gov.in/vahan4dashboard/vahan/view/reportview.xhtml"
        )

        # selecting y_axis entering maker as parameter
        find_element(browser, "id", "yaxisVar_label").click()
        time.sleep(1)
        find_element(browser, "id", "yaxisVar_4").click()
        time.sleep(2)

        # selecting x_axis entering fuel as parameter
        find_element(browser, "id", "xaxisVar_label").click()
        time.sleep(1)
        find_element(browser, "id", "xaxisVar_2").click()
        time.sleep(2)

        #  selecting year button and entering the value
        find_element(browser, "id", "selectedYear_label").click()
        time.sleep(1)
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
        find_element(browser, "id", "groupingTable:selectMonth").click()
        time.sleep(1)
        # Enter month
        find_element(
            browser,
            "xpath",
            f"//ul[@id='groupingTable:selectMonth_items']/li[text()='{month_label}']",
        ).click()
        time.sleep(2)

        # click on span toggler on left
        find_element(browser, "id", "filterLayout-toggler").click()
        time.sleep(1)

        # selecting adapted vehicle as category
        find_element(
            browser, "xpath", f"//label[text()='{vehicle_category_label}']"
        ).click()
        time.sleep(5)

        # click on refresh button on left toggler object
        find_element(
            browser,
            "css",
            "button[class='ui-button ui-widget ui-state-default ui-corner-all ui-button-text-icon-left']",
        ).click()
        time.sleep(5)

        # click on download button for downloading report
        find_element(browser, "id", "groupingTable:xls").click()
        time.sleep(5)
        browser.quit()

    except TimeoutException:
        browser.refresh()


def get_all_vehicle_category_elements():
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
    browserOpts.add_argument("--disable-single-click-autofill")
    browserOpts.set_capability("goog:loggingPrefs", {"performance": "ALL"})
    browser = webdriver.Chrome(options=browserOpts)

    try:
        browser.get(
            "https://vahan.parivahan.gov.in/vahan4dashboard/vahan/view/reportview.xhtml"
        )
        # click on span toggler on left
        find_element(browser, "id", "filterLayout-toggler").click()
        # find vehicle category element
        element = find_element(browser, "xpath", '//table[@id="VhClass"]/tbody')

        vehicle_category_lst = []
        for row_label in element.find_elements(By.TAG_NAME, 'tr'):
            vehicle_category_lst.append(row_label.text)
        return vehicle_category_lst

    except TimeoutException:
        browser.refresh()


def main():
    # extract_oem_data_by_vehicle_category(2024, "JAN", "ADAPTED VEHICLE")
    vehicle_category_lst = get_all_vehicle_category_elements()
    print(vehicle_category_lst)


if __name__ == "__main__":
    main()
