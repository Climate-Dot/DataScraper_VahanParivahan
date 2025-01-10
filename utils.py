import logging
import re
import os


from datetime import datetime, timedelta
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

# configure logging to write to both the console and a file
logging.basicConfig(
    level=logging.INFO,  # set the logging level (INFO, DEBUG, etc.)
    format='%(asctime)s - %(levelname)s - %(message)s',  # log message format
)

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
    "Lakshadweep",
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
    else:
        logging.info(f"Directory '{directory_path}' already exists.")
        pass

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
                EC.element_to_be_clickable((By.TAG_NAME, value))
            )
        else:
            raise ValueError(
                "Invalid identifier. Use 'id', 'css', 'tag' or 'xpath'."
            )
        return element
    except Exception as e:
        logging.error(f"Element not found {identifier}: {value}: {e}")
        raise e

def convert_date(date_str):
    return datetime.strptime(date_str, "%d/%b/%Y").strftime("%d/%m/%Y")

def remove_special_chars(text):
    return re.sub(r"\W+", " ", text)