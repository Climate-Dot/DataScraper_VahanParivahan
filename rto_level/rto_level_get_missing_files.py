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

from pipeline_logging import configure_pipeline_logging
from utils import *
from rto_level.rto_level_data_scraper import RTODataScraper

configure_pipeline_logging()

if len(sys.argv) > 2:
    month = sys.argv[1]
    year = sys.argv[2]
else:
    month, year = get_year_month_label()
rto_data_scraper = RTODataScraper()

with open("output.json", "r") as f:
    state_rto_mapping = json.load(f)


def get_missing_files():
    """Check for missing files and return a list of parameters for extraction."""
    parameters = []
    invalid_rto_labels = []
    for state in state_lst:
        all_rto_office_names = state_rto_mapping.get(state, [])
        for rto_office_name in all_rto_office_names:
            rto_folder_name = rto_data_scraper.build_rto_folder_name(rto_office_name)
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
                "reportTable.xlsx",
            )

            if not is_valid_excel_download(directory_path):
                parameters.append((state, rto_office_name, year, month))

    if invalid_rto_labels:
        sample_state, sample_label = invalid_rto_labels[0]
        logging.error(
            "Skipped %s invalid RTO labels while checking missing files. Example: state=%s label=%s",
            len(invalid_rto_labels),
            sample_state,
            sample_label,
        )

    return parameters


def get_existing_file_count():
    """Count valid .xlsx files for the target month/year and valid RTO labels only."""
    file_count = 0
    for state in state_lst:
        all_rto_office_names = state_rto_mapping.get(state, [])
        for rto_office_name in all_rto_office_names:
            rto_folder_name = rto_data_scraper.build_rto_folder_name(rto_office_name)
            if not rto_folder_name:
                continue

            file_path = os.path.join(
                os.getcwd(),
                "rto_level",
                "rto_level_ev_data",
                re.sub(r"[^a-zA-Z\s]", " ", state).rstrip(),
                rto_folder_name,
                str(year),
                month,
                "reportTable.xlsx",
            )
            if is_valid_excel_download(file_path):
                file_count += 1
    return file_count


def extract_missing_files():
    """Run the Selenium scraper in parallel for missing files."""
    parameters = get_missing_files()
    if not parameters:
        return False  # No missing files, stop execution

    logging.info(f"There are {len(parameters)} missing files. Extracting...")

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {
            executor.submit(rto_data_scraper.run_selenium, args): args
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
                    "Missing-file recovery failed for state=%s rto=%s year=%s month=%s: %s",
                    state_label,
                    rto_label,
                    year_label,
                    month_label,
                    e,
                )

    logging.info(
        "RTO missing-file recovery summary for %s %s: requested=%s succeeded=%s failed=%s",
        month,
        year,
        len(parameters),
        successful_downloads,
        len(failed_downloads),
    )

    return True  # Missing files were extracted


# Retry extraction until all files are downloaded
while True:
    expected_file_count = sum(
        1
        for rto_labels in state_rto_mapping.values()
        for rto_label in rto_labels
        if rto_data_scraper.build_rto_folder_name(rto_label)
    )
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
