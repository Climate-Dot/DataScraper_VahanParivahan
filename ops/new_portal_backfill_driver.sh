#!/usr/bin/env bash
# Runs the full backfill for a list of years, unattended, end to end.
#
#   YEARS="2023 2021 2020" nohup setsid bash ops/new_portal_backfill_driver.sh &
#
# Each year: fetch (sweep loop until complete) -> snapshot to blob -> ingest ->
# verify. Nothing waits on a human, which is the point: hand-driving the steps
# put a turn boundary between every phase and left the machine idle for hours
# between them (2026-09-29: 8h47m of dead time out of 15h elapsed, against ~6h
# of actual work).
#
# A year that cannot be completed is recorded and SKIPPED rather than retried
# forever — the portal's bad spells last hours, so the useful move is to press
# on and revisit later. The per-year checkpoint means a revisit resumes rather
# than restarts.
cd "$(dirname "$0")/.." || exit 1
mkdir -p run_logs

YEARS="${YEARS:?set YEARS, e.g. YEARS=\"2023 2021\"}"
SUMMARY=run_logs/backfill_summary.log
MAX="${MAX:-400}"
WAIT="${WAIT:-240}"
STALL_LIMIT="${STALL_LIMIT:-10}"

log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*" | tee -a "$SUMMARY"; }

already_loaded() {
  YEAR="$1" PYTHONPATH=. ./venv/bin/python - <<'PY' 2>/dev/null
import os, pyodbc
from runtime_config import load_config
cfg = load_config()["database"]
cn = pyodbc.connect(
    "DRIVER={ODBC Driver 18 for SQL Server};SERVER=%s;DATABASE=%s;UID=%s;PWD=%s;"
    "Encrypt=yes;TrustServerCertificate=yes" % (
        cfg["server"], cfg["database"], cfg["username"], cfg["password"]),
    autocommit=True)
cur = cn.cursor()
cur.execute("SELECT COUNT(*) FROM fact_ev_data_by_rto_v2 WHERE [year]=?",
            int(os.environ["YEAR"]))
print(cur.fetchone()[0])
PY
}

log "driver starting for years: $YEARS"

for YEAR in $YEARS; do
  existing=$(already_loaded "$YEAR")
  if [ -n "$existing" ] && [ "$existing" -gt 0 ] 2>/dev/null; then
    log "$YEAR: already has $existing rows in the database — skipping"
    continue
  fi

  log "$YEAR: fetch starting"
  YEAR="$YEAR" MAX="$MAX" WAIT="$WAIT" STALL_LIMIT="$STALL_LIMIT" \
    bash ops/new_portal_sweep_until_complete.sh > "run_logs/retry_loop_${YEAR}.log" 2>&1
  rc=$?
  sweeps=$(grep -cE "attempt [0-9]+ exit=" "run_logs/retry_loop_${YEAR}.log" 2>/dev/null || echo 0)

  if [ "$rc" -ne 0 ]; then
    reason=$(grep -oE "STALLED|gave up after [0-9]+ attempts" "run_logs/retry_loop_${YEAR}.log" | tail -1)
    log "$YEAR: fetch did NOT complete after $sweeps sweeps ($reason) — skipping, checkpoint kept"
    continue
  fi

  csv="new_portal_rto_ev_data_${YEAR}.csv"
  rows=$(( $(wc -l < "$csv") - 1 ))
  log "$YEAR: fetch complete in $sweeps sweeps, $rows rows"

  log "$YEAR: snapshotting"
  if ! ./venv/bin/python -m new_portal.rto_blob_snapshot --year "$YEAR" \
        >> "run_logs/snapshot_${YEAR}.log" 2>&1; then
    log "$YEAR: SNAPSHOT FAILED — not ingesting (an unarchived load is not repeatable)"
    continue
  fi

  log "$YEAR: ingesting $rows rows"
  if ./venv/bin/python -u -m new_portal.rto_ingest --year "$YEAR" \
       > "run_logs/ingest_${YEAR}.log" 2>&1; then
    loaded=$(already_loaded "$YEAR")
    log "$YEAR: DONE — $loaded rows in fact_ev_data_by_rto_v2"
  else
    log "$YEAR: INGEST FAILED — see run_logs/ingest_${YEAR}.log"
  fi
done

log "driver finished"
