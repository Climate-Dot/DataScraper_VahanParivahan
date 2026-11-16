#!/bin/bash
#
# V2 monthly RTO pipeline, shaped for cron.
#
# Not a variant of new_portal_rto_etl.sh. That script makes exactly one fetch
# attempt, which on this portal usually does not complete: availability is
# per-office and rotates over minutes, so rto_fetch correctly exits non-zero and
# a cron run of it would alert and ingest nothing. Worse, its first stage is a
# fetch, and rto_fetch deletes the checkpoint on a clean completion -- so
# "sweep until complete, then run the ETL script" re-pulls the whole year from
# zero and overwrites the complete CSV with a throttled partial. Measured
# 2026-11-04: a paced sweep finished 2026 in 1h29m with zero gaps, the ETL
# script invoked immediately after re-fetched for 87 minutes, overwrote the CSV
# with 123,693 rows and 314 gaps, and failed. This script therefore calls the
# sweep loop and then the post-sweep stages directly, and never shells out to
# new_portal_rto_etl.sh.
#
# Stage order is the contract:
#
#   fetch_sweep -> completeness_gate -> blob_snapshot -> ingestion
#               -> post_ingest_check -> staging_cleanup
#
# The gate sits between the sweep and the snapshot, not after it. Snapshotting a
# partial CSV would not corrupt anything on its own -- snapshots are immutable
# and a partial one is honest history -- but it would leave that partial as the
# newest blob, which then satisfies the off-VM watchdog and masks the very
# failure the watchdog exists to report.
#
# There is no dbt stage: no curated V2 model exists, so one would fail on a
# model that is not there.
#
# Usage:
#   bash new_portal_rto_monthly.sh             # previous calendar month
#   bash new_portal_rto_monthly.sh 2026 10     # re-run a specific month by hand

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

# One instance at a time. A degraded day-3 run can outlast two days, and the
# day-5 cron would otherwise start a second process against the same per-year
# checkpoint and the same CSV path. Exits 0 rather than failing, because "the
# previous run is still going" is information, not a fault.
LOCK_FILE="${PROJECT_ROOT}/new_portal_rto_monthly.lock"
exec 9>"${LOCK_FILE}"
if ! flock -n 9; then
    log_step "Another new_portal_rto_monthly.sh holds ${LOCK_FILE}; exiting."
    exit 0
fi

# The target is the previous, fully elapsed calendar month. Computed from year
# and month arithmetic rather than `date -d 'last month'`, which lands on the
# wrong month when today's day-of-month does not exist in the previous one.
if [ "$#" -ge 2 ]; then
    TARGET_YEAR="$1"
    TARGET_MONTH="$((10#$2))"
else
    current_year="$(date -u '+%Y')"
    current_month="$((10#$(date -u '+%m')))"
    if [ "${current_month}" -eq 1 ]; then
        TARGET_YEAR="$((current_year - 1))"
        TARGET_MONTH=12
    else
        TARGET_YEAR="${current_year}"
        TARGET_MONTH="$((current_month - 1))"
    fi
fi

# One portal call returns a whole year, so the unit of work is the year that
# contains the target month. In January that is last year, which finalises
# December; the new year is first fetched by February's run. Nothing consumes a
# three-day-old January, so the lag costs nothing.
#
# Taken before the sweep on purpose: the gate uses it as the floor the CSV's
# mtime must beat, which is what rejects a stale CSV left by an earlier failed
# run. A floor taken after the sweep would accept one written during it.
RUN_START_EPOCH="$(date -u '+%s')"

RUN_LABEL="$(describe_run_args "$@") -> ${TARGET_YEAR}-$(printf '%02d' "${TARGET_MONTH}")"
ETL_PIPELINE_NAME="new_portal_rto"
ETL_LOG_FILE="${PROJECT_ROOT}/new_portal_rto_monthly_logs.txt"
ETL_ALERT_DETAILS=""
ETL_ALERT_DETAILS_FILE=""
ETL_SUCCESS_DETAILS="New-portal RTO run completed for ${RUN_LABEL}."
CURRENT_STEP="bootstrap"
install_failure_alert_trap

# install_failure_alert_trap installs an ERR trap only, so a run killed rather
# than failed -- the VM powering off mid-sweep is the realistic case -- would be
# exactly as silent as the incident this pipeline exists to stop being silent
# about. systemd sends TERM before KILL on shutdown, so this fires.
trap 'CURRENT_STEP="${CURRENT_STEP}(killed by SIGTERM)"; etl_failure_trap_handler 143' TERM
trap 'CURRENT_STEP="${CURRENT_STEP}(killed by SIGINT)"; etl_failure_trap_handler 130' INT

log_step "New-portal RTO monthly run for ${RUN_LABEL}"

# The sweep's knobs are given explicit values rather than inherited, so the
# maximum wall-clock cost of a cron run is bounded and reviewable.
#
# SLEEP_SECONDS=0.45 is the measured-safe pacing and is not optional: the portal
# serves ~1,500 requests per 60s then hard-429s for ~20s, and nothing in
# new_portal/ handles 429 specially, so at the wrong rate every attempt dies at
# ~205 offices no matter how many attempts are allowed.
#
# MAX x WAIT bounds only the *waiting*: 30 x 300s is 2.5h of idle. It does not
# bound the run, because a failing attempt that walks most of the country costs
# nearly as much as a successful one, and STALL_LIMIT resets on any progress at
# all, so one office per attempt keeps the loop alive. The real ceiling is
# MAX x (attempt duration + WAIT), which at 30 min per attempt is ~17 hours.
# SWEEP_TIMEOUT is therefore what actually bounds the run, and it is set so the
# whole job finishes inside the 03:00-07:00 UTC window the crontab asks the VM
# to be up for. Raise it only together with that window.
SWEEP_TIMEOUT="${SWEEP_TIMEOUT:-3h}"
CURRENT_STEP="fetch_sweep"
log_step "Sweeping ${TARGET_YEAR} until complete (deadline ${SWEEP_TIMEOUT})"
YEAR="${TARGET_YEAR}" SLEEP_SECONDS=0.45 MAX=30 WAIT=300 STALL_LIMIT=6 \
    timeout "${SWEEP_TIMEOUT}" \
    bash "${PROJECT_ROOT}/ops/new_portal_sweep_until_complete.sh"

CURRENT_STEP="completeness_gate"
log_step "Gating ${TARGET_YEAR} on completeness before anything is snapshotted"
python3 -m new_portal.rto_completeness \
    --year "${TARGET_YEAR}" \
    --target-month "${TARGET_MONTH}" \
    --min-fetched-at "${RUN_START_EPOCH}"

CURRENT_STEP="blob_snapshot"
log_step "Snapshotting the ${TARGET_YEAR} CSV before ingesting it"
python3 -m new_portal.rto_blob_snapshot --year "${TARGET_YEAR}"

# Bulk first, row-by-row as a fallback: an expired SAS or a permissions change
# should cost time, not a month of data.
CURRENT_STEP="ingestion"
log_step "Ingesting ${TARGET_YEAR}"
python3 -m new_portal.rto_bulk_ingest --year "${TARGET_YEAR}" \
    || python3 -m new_portal.rto_ingest --year "${TARGET_YEAR}"

# --target-year/--target-month rather than letting the check recompute from
# today: a hand re-run of an older month would otherwise verify whatever month
# precedes today and fail a run whose data landed perfectly. --no-alert because
# the ERR trap below already posts one alert naming this step, and two alerts
# for one fault read as two faults with contradictory advice.
CURRENT_STEP="post_ingest_check"
log_step "Verifying the load landed and ${TARGET_YEAR}-${TARGET_MONTH} settled"
python3 "${PROJECT_ROOT}/ops/check_pipeline_freshness.py" \
    --table fact_ev_data_by_rto_v2 \
    --status-scope ALL_STATUSES \
    --max-load-age-days 35 \
    --min-month-total-ratio 0.85 \
    --target-year "${TARGET_YEAR}" \
    --target-month "${TARGET_MONTH}" \
    --pipeline new_portal_rto \
    --no-alert

# Last, and after the verification, so a failed check still leaves the loaded
# staging rows available for diagnosis.
# Not fatal. The data has already landed and been verified by this point, and
# clearing scratch tables is housekeeping -- its own module says it is not an
# ingestion-correctness fix -- so a transient error here must not mark the run
# FAILED and suppress the success alert for a month that loaded correctly.
CURRENT_STEP="staging_cleanup"
log_step "Clearing the V2 landing and staging tables"
python3 "${PROJECT_ROOT}/ops/truncate_v2_scratch_tables.py" \
    || log_step "WARNING: scratch cleanup failed; the next ingest truncates before loading, so this is not urgent."

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
