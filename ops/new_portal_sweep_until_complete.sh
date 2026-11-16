#!/usr/bin/env bash
# Sweeps the nationwide fetch for one year until it completes.
#
#   YEAR=2025 bash ops/new_portal_sweep_until_complete.sh
#
# Availability on this portal is per-office and rotates over minutes, so there
# is no "portal is up" state worth waiting for — a fixed-sample health probe
# reported 6/6 healthy while the sweep walked into a region failing 29 of 32.
# Instead: sweep, wait a courteous interval so offices rotate, sweep again.
#
# Each attempt resumes from that year's checkpoint and visits unvisited offices
# before gapped ones, so every sweep extends coverage and nothing that already
# succeeded is redone. Stops early if progress stalls.
cd "$(dirname "$0")/.." || exit 1
mkdir -p run_logs
YEAR=${YEAR:-$(date -u +%Y)}
MAX=${MAX:-60}
WAIT=${WAIT:-300}
STALL_LIMIT=${STALL_LIMIT:-4}
# This script exists only to run nationwide, multi-hour sweeps, which is exactly
# the case the portal's token bucket punishes. Measured 2026-11-03 on the VM: the
# portal serves ~1,500 requests per 60s (~25/s) then hard-429s for ~20s, and
# rto_fetch's own 0.2s default puts 8 workers at 26.7/s -- just over the line, so
# every attempt died at ~205 offices regardless of how many attempts were allowed.
# 0.45s is 16.5/s, a ~31% margin, measured over 150s of sustained load. The 0.2
# default stays correct for the short targeted runs rto_fetch is invoked for
# directly, which is why the override lives here rather than there.
SLEEP_SECONDS=${SLEEP_SECONDS:-0.45}

stalled=0
last=""
for i in $(seq 1 "$MAX"); do
  TS=$(date -u +%Y%m%dT%H%M%SZ)
  LOG="run_logs/attempt_${YEAR}_${i}_${TS}.log"
  echo "$LOG" > run_logs/CURRENT
  echo "=== ${YEAR} attempt $i/$MAX at $TS ==="
  ./venv/bin/python -u -m new_portal.rto_fetch --year "$YEAR" \
    --sleep-seconds "$SLEEP_SECONDS" > "$LOG" 2>&1
  RC=$?
  STATE=$(YEAR="$YEAR" PYTHONPATH=. ./venv/bin/python ops/new_portal_progress_count.py 2>/dev/null \
          || echo "count unavailable")
  echo "${YEAR} attempt $i exit=$RC | $STATE"

  [ "$RC" -eq 0 ] && { echo "COMPLETE after $i attempt(s)"; exit 0; }

  if [ "$STATE" = "$last" ]; then
    stalled=$((stalled + 1))
    echo "  no progress since last attempt ($stalled/$STALL_LIMIT)"
    [ "$stalled" -ge "$STALL_LIMIT" ] && {
      echo "STALLED: $STALL_LIMIT attempts with no change. Stopping."; exit 2; }
  else
    stalled=0
  fi
  last="$STATE"
  sleep "$WAIT"
done
echo "gave up after $MAX attempts"
exit 1
