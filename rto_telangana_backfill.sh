#!/bin/bash

set -eEuo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export VAHAN_HEADLESS="${VAHAN_HEADLESS:-false}"

log_step() {
    printf '%s - INFO - %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$1"
}

# shellcheck disable=SC1091
. "${PROJECT_ROOT}/venv/bin/activate"
# shellcheck disable=SC1091
. "${PROJECT_ROOT}/ops/etl_runtime.sh"

cd "${PROJECT_ROOT}"

RUN_LABEL="$(describe_run_args "$@")"
ETL_PIPELINE_NAME="rto_telangana_backfill"
ETL_LOG_FILE="${PROJECT_ROOT}/telangana_rto_backfill_logs.txt"
ETL_ALERT_DETAILS="Telangana historical RTO backfill failed. Check the dedicated backfill log for details."
ETL_ALERT_DETAILS_FILE=""
ETL_SUCCESS_DETAILS="Telangana historical backfill landed cleanly. The retro data convoy made it home."
CURRENT_STEP="bootstrap"
install_failure_alert_trap

log_step "Telangana historical RTO browser runtime VAHAN_HEADLESS=${VAHAN_HEADLESS}"
CURRENT_STEP="historical_backfill"
log_step "Starting Telangana historical RTO backfill for ${RUN_LABEL}"
run_selenium_step python3 rto_level/telangana_historical_backfill.py "$@"
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
