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
log_step "RTO browser runtime VAHAN_HEADLESS=${VAHAN_HEADLESS}"

# Run the Extraction
log_step "Starting RTO extraction for ${RUN_LABEL}"
run_selenium_step python3 rto_level/rto_level_data_scraper.py "$@"

# Run missing files extraction
log_step "Running RTO missing-file recovery for ${RUN_LABEL}"
run_selenium_step python3 rto_level/rto_level_get_missing_files.py "$@"

# Run Pre Processing
log_step "Running RTO preprocessing for ${RUN_LABEL}"
python3 rto_level/rto_level_data_pre_processing.py "$@"

# Ingestion
log_step "Running RTO ingestion for ${RUN_LABEL}"
python3 rto_level/rto_level_data_ingestion.py "$@"

# File Upload and Cleanup
log_step "Running RTO upload and cleanup for ${RUN_LABEL}"
python3 rto_level/upload_files_to_blob_storage.py "$@"

# Run dbt model
log_step "Running RTO dbt model for ${RUN_LABEL}"
cd "${PROJECT_ROOT}/climate_dot_dbt"
dbt run --select rto_wise_ev_data >> "${PROJECT_ROOT}/dbt_rto_wise_logs.txt" 2>&1
