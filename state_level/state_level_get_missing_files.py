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

from pipeline_constants import STATE_LIST
from pipeline_logging import configure_pipeline_logging
from runtime_config import resolve_month_year_args
from state_level.state_level_data_scraper import StateLevelDataScraper
from utils import is_valid_excel_download


configure_pipeline_logging()
logger = logging.getLogger(__name__)


def build_report_path(state, year, month):
    state_folder = re.sub(r"[^a-zA-Z\s]", " ", state).rstrip()
    return os.path.join(
        os.getcwd(),
        "state_level",
        "state_level_ev_data",
        state_folder,
        str(year),
        month,
        "reportTable.xlsx",
    )


def build_missing_parameters(month, year):
    parameters = []
    for state in STATE_LIST:
        if not is_valid_excel_download(build_report_path(state, year, month)):
            parameters.append((state, year, month))
    return parameters


def run_missing_file_recovery(scraper, parameters):
    successful_downloads = 0
    failed_downloads = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {
            executor.submit(scraper.run_selenium, args): args for args in parameters
        }
        for future in as_completed(futures):
            state, year, month = futures[future]
            try:
                future.result()
                successful_downloads += 1
            except Exception as exc:
                failed_downloads.append(state)
                logger.error(
                    "State missing-file recovery failed state=%s year=%s month=%s error=%s",
                    state,
                    year,
                    month,
                    exc,
                )

    return successful_downloads, failed_downloads


def main():
    month, year = resolve_month_year_args(sys.argv[1:])
    scraper = StateLevelDataScraper()

    parameters = build_missing_parameters(month, year)
    if not parameters:
        logger.info("No missing state files found for %s %s.", month, year)
        return

    logger.info(
        "Starting state missing-file recovery for %s %s with %s missing files.",
        month,
        year,
        len(parameters),
    )
    successful_downloads, failed_downloads = run_missing_file_recovery(
        scraper, parameters
    )

    remaining_missing = build_missing_parameters(month, year)
    logger.info(
        "State missing-file recovery summary for %s %s: requested=%s succeeded=%s failed=%s remaining=%s",
        month,
        year,
        len(parameters),
        successful_downloads,
        len(failed_downloads),
        len(remaining_missing),
    )

    if remaining_missing:
        raise RuntimeError(
            f"State missing-file recovery finished with {len(remaining_missing)} files still missing."
        )


if __name__ == "__main__":
    main()
