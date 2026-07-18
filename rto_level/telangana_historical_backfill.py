from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent

if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

from blob_storage_utils import (
    ensure_container_exists,
    upload_globbed_files_to_container,
    upload_matching_csv_artifact,
)
from pipeline_constants import MONTH_NAME_TO_NUMBER
from pipeline_logging import configure_pipeline_logging
from rto_level.rto_level_data_ingestion import RtoDataIngest
from rto_level.rto_level_data_pre_processing import RTOLevelDataPreProcessor
from rto_level.rto_level_data_scraper import (
    RTODataScraper,
    merge_state_rto_mappings,
)
from runtime_config import get_previous_month_year_label, load_config
from utils import is_valid_excel_download


configure_pipeline_logging()
logger = logging.getLogger(__name__)

TARGET_STATE = "Telangana"
MONTH_NUMBER_TO_NAME = {value: key for key, value in MONTH_NAME_TO_NUMBER.items()}
BACKFILL_ROOT = REPO_ROOT / "rto_level" / "historical_backfill" / "telangana"
BACKFILL_WORKSPACE = BACKFILL_ROOT / "raw_workspace"
BACKFILL_RAW_ROOT = BACKFILL_WORKSPACE / "rto_level" / "rto_level_ev_data"
BACKFILL_PROCESSED_ROOT = BACKFILL_ROOT / "processed"


@dataclass(frozen=True)
class MonthWindow:
    month: str
    year: str


def parse_args():
    previous_month, previous_year = get_previous_month_year_label()
    parser = argparse.ArgumentParser(
        description=(
            "Backfill historical Telangana RTO data into the raw SQL Server table "
            "using the current RTO mapping and the active preprocessing/ingestion code."
        )
    )
    parser.add_argument("--start-month", default="JAN")
    parser.add_argument("--start-year", default="2013")
    parser.add_argument("--end-month", default=previous_month)
    parser.add_argument("--end-year", default=previous_year)
    parser.add_argument("--max-workers", type=int, default=10)
    parser.add_argument(
        "--mapping-path",
        default="output.json",
        help="Path to the persisted state-to-RTO mapping JSON file.",
    )
    parser.add_argument(
        "--skip-mapping-persist",
        action="store_true",
        help="Refresh Telangana RTO offices live, but do not write them back to output.json.",
    )
    parser.add_argument(
        "--force-reextract",
        action="store_true",
        help="Ignore any existing valid Telangana workbooks and re-download everything in range.",
    )
    parser.add_argument(
        "--upload-to-blob",
        action="store_true",
        help="Upload Telangana raw XLSX files and generated CSVs to blob storage after each month is ingested.",
    )
    parser.add_argument(
        "--run-dbt-full-refresh",
        action="store_true",
        help=(
            "Run `dbt run --full-refresh --select rto_wise_ev_data` after the raw backfill. "
            "This is off by default because it touches the curated model."
        ),
    )
    return parser.parse_args()


def normalize_month_label(month_label: str) -> str:
    normalized = str(month_label).strip().upper()
    if normalized not in MONTH_NAME_TO_NUMBER:
        raise ValueError(f"Unsupported month label: {month_label}")
    return normalized


def build_month_windows(start_month: str, start_year: str, end_month: str, end_year: str):
    start_month = normalize_month_label(start_month)
    end_month = normalize_month_label(end_month)
    start_key = (int(start_year), MONTH_NAME_TO_NUMBER[start_month])
    end_key = (int(end_year), MONTH_NAME_TO_NUMBER[end_month])
    if start_key > end_key:
        raise ValueError("Start month/year must be earlier than or equal to end month/year.")

    windows = []
    current_year, current_month_number = start_key
    while (current_year, current_month_number) <= end_key:
        windows.append(
            MonthWindow(
                month=MONTH_NUMBER_TO_NAME[current_month_number],
                year=str(current_year),
            )
        )
        current_month_number += 1
        if current_month_number > 12:
            current_month_number = 1
            current_year += 1
    return windows


def load_mapping(mapping_path: Path):
    if not mapping_path.exists():
        return {}
    with mapping_path.open("r", encoding="utf-8") as mapping_file:
        return json.load(mapping_file)


def persist_mapping(mapping_path: Path, mapping_payload):
    with mapping_path.open("w", encoding="utf-8") as mapping_file:
        json.dump(mapping_payload, mapping_file, indent=4)


def refresh_telangana_mapping(scraper: RTODataScraper, mapping_path: Path, persist: bool):
    previous_mapping = load_mapping(mapping_path)
    fresh_mapping = scraper.get_all_rto_from_state(TARGET_STATE) or {}
    merged_mapping = merge_state_rto_mappings(previous_mapping, fresh_mapping)

    telangana_offices = merged_mapping.get(TARGET_STATE, [])
    if not telangana_offices:
        raise RuntimeError(
            "No Telangana RTO offices were available after the mapping refresh."
        )

    if persist:
        persist_mapping(mapping_path, merged_mapping)
        logger.info(
            "Persisted refreshed Telangana mapping to %s with %s offices.",
            mapping_path,
            len(telangana_offices),
        )
    else:
        logger.info(
            "Using refreshed Telangana mapping in-memory only with %s offices.",
            len(telangana_offices),
        )

    return telangana_offices


def build_report_path(scraper: RTODataScraper, rto_office_name: str, month: str, year: str):
    rto_folder_name = scraper.build_rto_folder_name(rto_office_name)
    if not rto_folder_name:
        return None
    return (
        BACKFILL_RAW_ROOT
        / TARGET_STATE
        / rto_folder_name
        / year
        / month
        / "reportTable.xlsx"
    )


def build_download_parameters(scraper, telangana_offices, month, year, force_reextract):
    parameters = []
    invalid_labels = []
    for rto_office_name in telangana_offices:
        report_path = build_report_path(scraper, rto_office_name, month, year)
        if report_path is None:
            invalid_labels.append(rto_office_name)
            continue
        if force_reextract or not is_valid_excel_download(report_path):
            parameters.append((TARGET_STATE, rto_office_name, year, month))
    return parameters, invalid_labels


def count_valid_reports(scraper, telangana_offices, month, year):
    valid_count = 0
    for rto_office_name in telangana_offices:
        report_path = build_report_path(scraper, rto_office_name, month, year)
        if report_path and is_valid_excel_download(report_path):
            valid_count += 1
    return valid_count


def run_download_batch(scraper, parameters, max_workers):
    successful_downloads = 0
    failed_downloads = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(scraper.run_selenium, args): args for args in parameters
        }
        for future in as_completed(futures):
            state, rto_label, year, month = futures[future]
            try:
                future.result()
                successful_downloads += 1
            except Exception as exc:
                failed_downloads.append((state, rto_label, exc))
                logger.error(
                    "Telangana historical download failed state=%s rto=%s year=%s month=%s failed_step=%s diagnostics=%s error=%s",
                    state,
                    rto_label,
                    year,
                    month,
                    getattr(exc, "step", "download_flow"),
                    getattr(exc, "diagnostics", {}).get("metadata_path", ""),
                    exc,
                )

    return successful_downloads, failed_downloads


def ensure_month_downloads(scraper, telangana_offices, month, year, *, max_workers, force_reextract):
    previous_valid_count = -1
    while True:
        parameters, invalid_labels = build_download_parameters(
            scraper,
            telangana_offices,
            month,
            year,
            force_reextract=force_reextract,
        )
        if invalid_labels:
            logger.error(
                "Skipped %s Telangana RTO labels that could not be converted into folder names.",
                len(invalid_labels),
            )

        if not parameters:
            logger.info(
                "All Telangana RTO workbooks are already present for %s %s.",
                month,
                year,
            )
            return

        expected_count = sum(
            1
            for rto_office_name in telangana_offices
            if scraper.build_rto_folder_name(rto_office_name)
        )
        existing_count = count_valid_reports(scraper, telangana_offices, month, year)
        logger.info(
            "Telangana historical download status for %s %s: expected=%s existing=%s missing=%s",
            month,
            year,
            expected_count,
            existing_count,
            len(parameters),
        )

        successful_downloads, failed_downloads = run_download_batch(
            scraper,
            parameters,
            max_workers=max_workers,
        )
        refreshed_count = count_valid_reports(scraper, telangana_offices, month, year)
        remaining_missing, _ = build_download_parameters(
            scraper,
            telangana_offices,
            month,
            year,
            force_reextract=False,
        )

        logger.info(
            "Telangana historical download summary for %s %s: requested=%s succeeded=%s failed=%s remaining=%s",
            month,
            year,
            len(parameters),
            successful_downloads,
            len(failed_downloads),
            len(remaining_missing),
        )

        if not remaining_missing:
            return

        if refreshed_count <= existing_count or refreshed_count == previous_valid_count:
            raise RuntimeError(
                f"Telangana historical backfill made no download progress for {month} {year}; "
                f"{len(remaining_missing)} workbooks are still missing."
            )

        previous_valid_count = refreshed_count
        time.sleep(5)
        force_reextract = False


def build_output_csv_path(month, year):
    BACKFILL_PROCESSED_ROOT.mkdir(parents=True, exist_ok=True)
    return BACKFILL_PROCESSED_ROOT / f"rto_level_ev_data_telangana_{month}_{year}.csv"


def upload_telangana_artifacts(month, year, csv_path):
    from azure.storage.blob import BlobServiceClient

    config = load_config()
    blob_service_client = BlobServiceClient.from_connection_string(
        config["storage"]["connection_string"]
    )
    raw_container_name = config["storage"]["rto_wise_container_name"]
    csv_container_name = config["storage"]["rto_wise_csv_container_name"]

    raw_container_client = blob_service_client.get_container_client(raw_container_name)
    csv_container_client = blob_service_client.get_container_client(csv_container_name)

    ensure_container_exists(raw_container_client, raw_container_name)
    ensure_container_exists(csv_container_client, csv_container_name)

    raw_file_pattern = str(
        BACKFILL_RAW_ROOT
        / TARGET_STATE
        / "*"
        / year
        / month
        / "*.xlsx"
    )
    import glob

    raw_file_list = glob.glob(raw_file_pattern)
    upload_globbed_files_to_container(
        raw_file_list,
        relative_root=str(BACKFILL_RAW_ROOT),
        container_client=raw_container_client,
    )
    upload_matching_csv_artifact(
        processed_file_directory=str(BACKFILL_PROCESSED_ROOT),
        csv_prefix=csv_path.stem,
        csv_container_client=csv_container_client,
        csv_container_name=csv_container_name,
    )


def run_dbt_full_refresh():
    dbt_logs_path = REPO_ROOT / "dbt_rto_wise_logs.txt"
    with dbt_logs_path.open("a", encoding="utf-8") as dbt_logs:
        subprocess.run(
            ["dbt", "run", "--full-refresh", "--select", "rto_wise_ev_data"],
            cwd=REPO_ROOT / "climate_dot_dbt",
            check=True,
            stdout=dbt_logs,
            stderr=subprocess.STDOUT,
            text=True,
        )


def main():
    args = parse_args()
    mapping_path = Path(args.mapping_path)
    if not mapping_path.is_absolute():
        mapping_path = (REPO_ROOT / mapping_path).resolve()
    month_windows = build_month_windows(
        args.start_month,
        args.start_year,
        args.end_month,
        args.end_year,
    )
    logger.info(
        "Preparing Telangana historical RTO backfill from %s %s through %s %s.",
        month_windows[0].month,
        month_windows[0].year,
        month_windows[-1].month,
        month_windows[-1].year,
    )

    scraper = RTODataScraper()
    telangana_offices = refresh_telangana_mapping(
        scraper,
        mapping_path,
        persist=not args.skip_mapping_persist,
    )

    BACKFILL_WORKSPACE.mkdir(parents=True, exist_ok=True)
    BACKFILL_PROCESSED_ROOT.mkdir(parents=True, exist_ok=True)
    os.chdir(BACKFILL_WORKSPACE)

    preprocessor = RTOLevelDataPreProcessor(
        base_directory=REPO_ROOT,
        mapping_file_path=REPO_ROOT / "Table and Mapping V2.xlsx",
        raw_files_directory=BACKFILL_RAW_ROOT,
    )
    ingester = RtoDataIngest()
    processed_months = 0

    for window in month_windows:
        logger.info(
            "Starting Telangana historical month=%s year=%s offices=%s",
            window.month,
            window.year,
            len(telangana_offices),
        )
        ensure_month_downloads(
            scraper,
            telangana_offices,
            window.month,
            window.year,
            max_workers=args.max_workers,
            force_reextract=args.force_reextract,
        )

        final_df = preprocessor.data_preprocessing(
            window.month,
            window.year,
            states=[TARGET_STATE],
        )
        output_csv_path = build_output_csv_path(window.month, window.year)
        final_df.to_csv(output_csv_path, header=True, index=False)
        logger.info(
            "Prepared Telangana RTO CSV %s rows=%s",
            output_csv_path.name,
            len(final_df),
        )

        if final_df.empty:
            logger.warning(
                "No Telangana rows were produced for %s %s. Skipping SQL Server ingestion for this month.",
                window.month,
                window.year,
            )
        else:
            inserted_rows = ingester.data_ingest_from_file(str(output_csv_path))
            logger.info(
                "Inserted or refreshed %s Telangana raw rows for %s %s.",
                inserted_rows,
                window.month,
                window.year,
            )

        if args.upload_to_blob:
            upload_telangana_artifacts(window.month, window.year, output_csv_path)
            logger.info(
                "Uploaded Telangana raw workbooks and CSV artifact for %s %s.",
                window.month,
                window.year,
            )

        processed_months += 1

    if args.run_dbt_full_refresh:
        logger.warning(
            "Running dbt full-refresh for rto_wise_ev_data after Telangana raw backfill."
        )
        run_dbt_full_refresh()

    logger.info(
        "Completed Telangana historical backfill for %s month windows.",
        processed_months,
    )


if __name__ == "__main__":
    main()
