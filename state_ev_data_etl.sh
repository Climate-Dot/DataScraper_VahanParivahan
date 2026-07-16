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
log_step "State browser runtime VAHAN_HEADLESS=${VAHAN_HEADLESS}"

# Run the Extraction
log_step "Starting state extraction for ${RUN_LABEL}"
run_selenium_step python3 State-level/state_level_data_scraper.py "$@"

# Run missing files extraction
log_step "Running state missing-file recovery for ${RUN_LABEL}"
run_selenium_step python3 State-level/state_level_get_missing_files.py "$@"

# Run Pre Processing
log_step "Running state preprocessing for ${RUN_LABEL}"
python3 State-level/state_level_data_pre_processing.py "$@"

# Ingestion
log_step "Running state ingestion for ${RUN_LABEL}"
python3 State-level/state_level_data_ingestion.py "$@"

# File Upload and Cleanup
log_step "Running state upload and cleanup for ${RUN_LABEL}"
python3 State-level/upload_files_to_blob_storage.py "$@"
