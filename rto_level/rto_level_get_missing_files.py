import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Add repository path to sys.path
repo_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if repo_path not in sys.path:
    sys.path.append(repo_path)

from utils import *
from rto_level.rto_level_data_scraper import RTODataScraper

month, year = get_year_month_label()
rto_data_scraper = RTODataScraper()

with open("output.json", "r") as f:
    state_rto_mapping = json.load(f)


def get_missing_files():
    """Check for missing files and return a list of parameters for extraction."""
    parameters = []
    for state in state_lst:
        all_rto_office_names = state_rto_mapping.get(state, [])
        for rto_office_name in all_rto_office_names:
            directory_path = os.path.join(
                os.getcwd(),
                "rto_level",
                "rto_level_ev_data",
                re.sub(r"[^a-zA-Z\s]", " ", state).rstrip(),
                rto_data_scraper.extract_rto_name_and_code(rto_office_name),
                str(year),
                month,
                "reportTable.xlsx",
            )

            if not os.path.exists(directory_path):
                parameters.append((state, rto_office_name, year, month))

    return parameters


def get_existing_file_count():
    """Count the number of .xlsx files in the output directory."""
    file_count = 0
    directory = os.path.join(os.getcwd(), "rto_level", "rto_level_ev_data")
    for root, _, files in os.walk(directory):
        file_count += sum(1 for file in files if file.endswith(".xlsx"))
    return file_count


def extract_missing_files():
    """Run the Selenium scraper in parallel for missing files."""
    parameters = get_missing_files()
    if not parameters:
        return False  # No missing files, stop execution

    logging.info(f"There are {len(parameters)} missing files. Extracting...")

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(rto_data_scraper.run_selenium, args) for args in parameters
        ]

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.warning(f"Exception occurred: {e}")

    return True  # Missing files were extracted


# Retry extraction until all files are downloaded
while True:
    expected_file_count = sum(len(v) for v in state_rto_mapping.values())
    existing_file_count = get_existing_file_count()

    if existing_file_count == expected_file_count:
        logging.info("All files have been successfully extracted!")
        break  # Exit loop if all files are present

    logging.warning(
        f"Files missing! Expected: {expected_file_count}, Found: {existing_file_count}"
    )

    success = extract_missing_files()
    if not success:
        logging.error("No new files were extracted. Exiting.")
        break  # Prevent infinite loop if extraction isn't resolving the issue

    logging.info("Retrying file check...")
    time.sleep(5)  # Small delay before rechecking
