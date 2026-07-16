#!/bin/bash

set -e

PROJECT_ROOT=/home/climate_dot_data/DataScraper_VahanParivahan

log_step() {
    printf '%s - INFO - %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$1"
}

export VAHAN_HEADLESS="${VAHAN_HEADLESS:-false}"

# shellcheck disable=SC1091
. "${PROJECT_ROOT}/venv/bin/activate"
# shellcheck disable=SC1091
. "${PROJECT_ROOT}/ops/etl_runtime.sh"

cd "${PROJECT_ROOT}"

RUN_LABEL="$(describe_run_args "$@")"
log_step "OEM browser runtime VAHAN_HEADLESS=${VAHAN_HEADLESS}"

# Run the Extraction
log_step "Starting OEM extraction for ${RUN_LABEL}"
run_selenium_step python3 OEM-level/oem_level_data_scraper.py "$@"

# Run missing files extraction
log_step "Running OEM missing-file recovery for ${RUN_LABEL}"
run_selenium_step python3 OEM-level/get_missing_files.py "$@"

# Run Pre Processing
log_step "Running OEM preprocessing for ${RUN_LABEL}"
python3 OEM-level/data_preprocessing_v2.py "$@"

# Ingestion
log_step "Running OEM ingestion for ${RUN_LABEL}"
python3 OEM-level/data_ingestion.py "$@"

# File Upload and Cleanup
log_step "Running OEM upload and cleanup for ${RUN_LABEL}"
python3 OEM-level/upload_files_to_blob_storage.py "$@"

# Run dbt model
log_step "Running OEM dbt model for ${RUN_LABEL}"
cd "${PROJECT_ROOT}/climate_dot_dbt"
dbt run --select oem_wise_ev_data >> "${PROJECT_ROOT}/dbt_oem_wise_logs.txt" 2>&1
