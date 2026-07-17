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
ETL_PIPELINE_NAME="oem"
ETL_LOG_FILE="${PROJECT_ROOT}/oem_etl_logs.txt"
ETL_ALERT_DETAILS=""
ETL_ALERT_DETAILS_FILE=""
ETL_SUCCESS_DETAILS="Fresh OEM data made it all the way home. Fleet mood: immaculate."
CURRENT_STEP="bootstrap"
install_failure_alert_trap

log_step "OEM browser runtime VAHAN_HEADLESS=${VAHAN_HEADLESS}"

# Run the Extraction
CURRENT_STEP="extraction"
log_step "Starting OEM extraction for ${RUN_LABEL}"
run_selenium_step python3 oem_level/oem_level_data_scraper.py "$@"

# Run missing files extraction
CURRENT_STEP="missing_file_recovery"
log_step "Running OEM missing-file recovery for ${RUN_LABEL}"
run_selenium_step python3 oem_level/get_missing_files.py "$@"

# Run Pre Processing
CURRENT_STEP="preprocessing"
log_step "Running OEM preprocessing for ${RUN_LABEL}"
python3 oem_level/data_preprocessing_v2.py "$@"

# Ingestion
CURRENT_STEP="ingestion"
log_step "Running OEM ingestion for ${RUN_LABEL}"
python3 oem_level/data_ingestion.py "$@"

# File Upload and Cleanup
CURRENT_STEP="blob_upload"
log_step "Running OEM upload and cleanup for ${RUN_LABEL}"
python3 oem_level/upload_files_to_blob_storage.py "$@"

# Run dbt model
CURRENT_STEP="dbt_model"
ETL_ALERT_DETAILS="dbt model failure. Check the ETL log and dbt excerpt below."
ETL_ALERT_DETAILS_FILE="${PROJECT_ROOT}/dbt_oem_wise_logs.txt"
log_step "Running OEM dbt model for ${RUN_LABEL}"
cd "${PROJECT_ROOT}/climate_dot_dbt"
dbt run --select oem_wise_ev_data >> "${PROJECT_ROOT}/dbt_oem_wise_logs.txt" 2>&1
CURRENT_STEP="completed"

set +e
send_success_alert
alert_exit_code="$?"
set -e

if [ "${alert_exit_code}" -ne 0 ]; then
    printf '%s - WARNING - Google Chat success alert could not be delivered for pipeline=%s.\n' \
        "$(date '+%Y-%m-%d %H:%M:%S')" \
        "${ETL_PIPELINE_NAME:-unknown}" >&2
fi
