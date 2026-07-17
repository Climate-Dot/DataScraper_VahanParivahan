import json
import logging
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


SCRIPT_DIR = os.path.dirname(__file__)
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))

if REPO_ROOT not in sys.path:
    sys.path.append(REPO_ROOT)

from pipeline_constants import STATE_LIST
from pipeline_logging import configure_pipeline_logging
from rto_level.rto_level_data_scraper import RTODataScraper
from runtime_config import resolve_month_year_args
from utils import is_valid_excel_download


configure_pipeline_logging()
logger = logging.getLogger(__name__)


def load_state_rto_mapping(mapping_path="output.json"):
    with open(mapping_path, "r", encoding="utf-8") as mapping_file:
        return json.load(mapping_file)


def build_report_path(scraper, state, rto_office_name, year, month):
    rto_folder_name = scraper.build_rto_folder_name(rto_office_name)
    if not rto_folder_name:
        return None

    state_folder = re.sub(r"[^a-zA-Z\s]", " ", state).rstrip()
    return os.path.join(
        os.getcwd(),
        "rto_level",
        "rto_level_ev_data",
        state_folder,
        rto_folder_name,
        str(year),
        month,
        "reportTable.xlsx",
    )


def build_missing_parameters(scraper, state_rto_mapping, month, year):
    parameters = []
    invalid_rto_labels = []

    for state in STATE_LIST:
        for rto_office_name in state_rto_mapping.get(state, []):
            report_path = build_report_path(scraper, state, rto_office_name, year, month)
            if not report_path:
                invalid_rto_labels.append((state, rto_office_name))
                continue
            if not is_valid_excel_download(report_path):
                parameters.append((state, rto_office_name, year, month))

    return parameters, invalid_rto_labels


def count_existing_valid_files(scraper, state_rto_mapping, month, year):
    valid_file_count = 0
    for state in STATE_LIST:
        for rto_office_name in state_rto_mapping.get(state, []):
            report_path = build_report_path(scraper, state, rto_office_name, year, month)
            if report_path and is_valid_excel_download(report_path):
                valid_file_count += 1
    return valid_file_count


def run_missing_file_recovery(scraper, parameters):
    successful_downloads = 0
    failed_downloads = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {
            executor.submit(scraper.run_selenium, args): args for args in parameters
        }
        for future in as_completed(futures):
            state, rto_label, year, month = futures[future]
            try:
                future.result()
                successful_downloads += 1
            except Exception as exc:
                failed_downloads.append((state, rto_label))
                logger.error(
                    "RTO missing-file recovery failed state=%s rto=%s year=%s month=%s error=%s",
                    state,
                    rto_label,
                    year,
                    month,
                    exc,
                )

    return successful_downloads, failed_downloads


def main():
    month, year = resolve_month_year_args(sys.argv[1:])
    scraper = RTODataScraper()
    state_rto_mapping = load_state_rto_mapping()

    previous_existing_count = -1
    while True:
        parameters, invalid_rto_labels = build_missing_parameters(
            scraper, state_rto_mapping, month, year
        )
        if invalid_rto_labels:
            sample_state, sample_label = invalid_rto_labels[0]
            logger.error(
                "Skipped %s invalid RTO labels while checking missing files. Example: state=%s label=%s",
                len(invalid_rto_labels),
                sample_state,
                sample_label,
            )

        if not parameters:
            logger.info("No missing RTO files found for %s %s.", month, year)
            return

        expected_file_count = sum(
            1
            for state in STATE_LIST
            for rto_office_name in state_rto_mapping.get(state, [])
            if scraper.build_rto_folder_name(rto_office_name)
        )
        existing_file_count = count_existing_valid_files(
            scraper, state_rto_mapping, month, year
        )

        if existing_file_count == expected_file_count:
            logger.info("All RTO files are already present for %s %s.", month, year)
            return

        logger.warning(
            "RTO files missing for %s %s: expected=%s existing=%s missing=%s",
            month,
            year,
            expected_file_count,
            existing_file_count,
            len(parameters),
        )

        successful_downloads, failed_downloads = run_missing_file_recovery(
            scraper, parameters
        )
        refreshed_existing_count = count_existing_valid_files(
            scraper, state_rto_mapping, month, year
        )
        remaining_missing, _ = build_missing_parameters(
            scraper, state_rto_mapping, month, year
        )

        logger.info(
            "RTO missing-file recovery summary for %s %s: requested=%s succeeded=%s failed=%s remaining=%s",
            month,
            year,
            len(parameters),
            successful_downloads,
            len(failed_downloads),
            len(remaining_missing),
        )

        if not remaining_missing:
            return

        if refreshed_existing_count <= existing_file_count or refreshed_existing_count == previous_existing_count:
            raise RuntimeError(
                f"RTO missing-file recovery made no progress for {month} {year}; {len(remaining_missing)} files are still missing."
            )

        previous_existing_count = refreshed_existing_count
        logger.info("Retrying RTO missing-file check for %s %s after a short pause.", month, year)
        time.sleep(5)


if __name__ == "__main__":
    main()
