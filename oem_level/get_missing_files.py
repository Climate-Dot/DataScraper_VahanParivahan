import logging
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed


SCRIPT_DIR = os.path.dirname(__file__)
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))

if SCRIPT_DIR not in sys.path:
    sys.path.append(SCRIPT_DIR)
if REPO_ROOT not in sys.path:
    sys.path.append(REPO_ROOT)

from oem_level.oem_level_data_scraper import OEMDataScraper
from pipeline_constants import STATE_LIST
from pipeline_logging import configure_pipeline_logging
from runtime_config import resolve_month_year_args
from utils import is_valid_excel_download


configure_pipeline_logging()
logger = logging.getLogger(__name__)


def build_report_path(state, category, year, month):
    state_folder = re.sub(r"[^a-zA-Z\s]", " ", state).rstrip()
    category_folder = re.sub(r"\W+", " ", category).rstrip()
    return os.path.join(
        os.getcwd(),
        "oem_level",
        "oem_data_by_state_and_category",
        state_folder,
        category_folder,
        str(year),
        month,
        "reportTable.xlsx",
    )


def build_missing_parameters(vehicle_categories, month, year):
    parameters = []
    for state in STATE_LIST:
        for category in vehicle_categories:
            if not is_valid_excel_download(build_report_path(state, category, year, month)):
                parameters.append((state, year, month, category))
    return parameters


def run_missing_file_recovery(scraper, parameters):
    successful_downloads = 0
    failed_downloads = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {
            executor.submit(scraper.run_selenium, args): args for args in parameters
        }
        for future in as_completed(futures):
            state, year, month, category = futures[future]
            try:
                future.result()
                successful_downloads += 1
            except Exception as exc:
                failed_downloads.append((state, category))
                logger.error(
                    "OEM missing-file recovery failed state=%s year=%s month=%s vehicle_category=%s error=%s",
                    state,
                    year,
                    month,
                    category,
                    exc,
                )

    return successful_downloads, failed_downloads


def main():
    month, year = resolve_month_year_args(sys.argv[1:])
    scraper = OEMDataScraper()

    vehicle_categories = scraper.get_all_vehicle_category_elements()
    if not vehicle_categories:
        raise RuntimeError(
            "Unable to fetch OEM vehicle categories from Vahan. Missing-file recovery cannot continue."
        )

    parameters = build_missing_parameters(vehicle_categories, month, year)
    if not parameters:
        logger.info("No missing OEM files found for %s %s.", month, year)
        return

    logger.info(
        "Starting OEM missing-file recovery for %s %s with %s missing files.",
        month,
        year,
        len(parameters),
    )
    successful_downloads, failed_downloads = run_missing_file_recovery(
        scraper, parameters
    )

    remaining_missing = build_missing_parameters(vehicle_categories, month, year)
    logger.info(
        "OEM missing-file recovery summary for %s %s: requested=%s succeeded=%s failed=%s remaining=%s",
        month,
        year,
        len(parameters),
        successful_downloads,
        len(failed_downloads),
        len(remaining_missing),
    )

    if remaining_missing:
        raise RuntimeError(
            f"OEM missing-file recovery finished with {len(remaining_missing)} files still missing."
        )


if __name__ == "__main__":
    main()
