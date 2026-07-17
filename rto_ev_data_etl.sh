#!/bin/bash

set -eE

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
ETL_PIPELINE_NAME="rto"
ETL_LOG_FILE="${PROJECT_ROOT}/rto_ev_etl_logs.txt"
ETL_ALERT_DETAILS=""
CURRENT_STEP="bootstrap"
install_failure_alert_trap

log_step "RTO browser runtime VAHAN_HEADLESS=${VAHAN_HEADLESS}"

# Run the Extraction
CURRENT_STEP="extraction"
log_step "Starting RTO extraction for ${RUN_LABEL}"
run_selenium_step python3 rto_level/rto_level_data_scraper.py "$@"

# Run missing files extraction
CURRENT_STEP="missing_file_recovery"
log_step "Running RTO missing-file recovery for ${RUN_LABEL}"
run_selenium_step python3 rto_level/rto_level_get_missing_files.py "$@"

# Run Pre Processing
CURRENT_STEP="preprocessing"
log_step "Running RTO preprocessing for ${RUN_LABEL}"
python3 rto_level/rto_level_data_pre_processing.py "$@"

# Ingestion
CURRENT_STEP="ingestion"
log_step "Running RTO ingestion for ${RUN_LABEL}"
python3 rto_level/rto_level_data_ingestion.py "$@"

# File Upload and Cleanup
CURRENT_STEP="blob_upload"
log_step "Running RTO upload and cleanup for ${RUN_LABEL}"
python3 rto_level/upload_files_to_blob_storage.py "$@"

# Run dbt model
CURRENT_STEP="dbt_model"
ETL_ALERT_DETAILS="Detailed dbt output: ${PROJECT_ROOT}/dbt_rto_wise_logs.txt"
log_step "Running RTO dbt model for ${RUN_LABEL}"
cd "${PROJECT_ROOT}/climate_dot_dbt"
dbt run --select rto_wise_ev_data >> "${PROJECT_ROOT}/dbt_rto_wise_logs.txt" 2>&1
CURRENT_STEP="completed"
