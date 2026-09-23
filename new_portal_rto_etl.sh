e#!/bin/bash
#
# V2 monthly RTO pipeline, against the new Parivahan Analytics portal.
#
# Deliberately shorter than rto_ev_data_etl.sh: the new portal is a JSON API, so
# there is no Selenium, no xvfb, no browser-runtime policy, no missing-file
# recovery pass and no XLSX preprocessing. Fetch, ingest, done.
#
# One fetch covers a whole calendar year (see new_portal/rto_fetch.py), so this
# re-pulls the year to date every run and replaces it wholesale. That is what
# absorbs backdated registrations landing in already-ingested months.
#
# Usage:
#   bash new_portal_rto_etl.sh            # current year
#   bash new_portal_rto_etl.sh 2026       # explicit year

set -eE

PROJECT_ROOT=/home/climate_dot_data/DataScraper_VahanParivahan

log_step() {
    printf '%s - INFO - %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$1"
}

# shellcheck disable=SC1091
. "${PROJECT_ROOT}/venv/bin/activate"
# shellcheck disable=SC1091
. "${PROJECT_ROOT}/ops/etl_runtime.sh"

cd "${PROJECT_ROOT}"

TARGET_YEAR="${1:-$(date '+%Y')}"

ETL_PIPELINE_NAME="new_portal_rto"
ETL_LOG_FILE="${PROJECT_ROOT}/new_portal_rto_etl_logs.txt"
ETL_ALERT_DETAILS=""
ETL_ALERT_DETAILS_FILE=""
ETL_SUCCESS_DETAILS="New-portal RTO run finished cleanly for ${TARGET_YEAR}."
CURRENT_STEP="bootstrap"
install_failure_alert_trap

# Fetch. Exits non-zero on a partial pull, which the `set -e` above turns into a
# failed run + alert — a partial CSV must never reach ingestion, because
# ingestion replaces the whole year's snapshot.
CURRENT_STEP="fetch"
log_step "Fetching new-portal RTO data for ${TARGET_YEAR}"
python3 -m new_portal.rto_fetch --year "${TARGET_YEAR}"

# Snapshot the CSV to blob storage BEFORE ingesting. The portal revises past
# months (measured: two RTOs moved +0.6% and +0.9% between consecutive daily
# runs), and ingestion replaces the year wholesale, so the database only ever
# holds the latest pull. These immutable per-run snapshots are the only record
# of what we believed at a given moment. Uploading before ingestion means a
# snapshot exists even if ingestion then fails.
CURRENT_STEP="blob_snapshot"
log_step "Snapshotting new-portal RTO CSV for ${TARGET_YEAR}"
python3 -m new_portal.rto_blob_snapshot --year "${TARGET_YEAR}"

CURRENT_STEP="ingestion"
log_step "Ingesting new-portal RTO data for ${TARGET_YEAR}"
python3 -m new_portal.rto_ingest --year "${TARGET_YEAR}"

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
