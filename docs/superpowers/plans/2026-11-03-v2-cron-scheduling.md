# V2 Cron Scheduling Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Put the V2 new-portal RTO pipeline on a production schedule with the same alerting discipline as the three V1 Vahan pipelines, and close the failure mode that lost the Nov 1 2026 window — a cron job that never fires sends no alert, so silence was indistinguishable from success.

**Architecture:** A new cron-only entrypoint (`new_portal_rto_monthly.sh`) wraps the existing sweep-until-complete loop, then runs an **independent, blocking completeness gate** before anything is snapshotted or ingested. Detection of a run that never happened is deliberately split in two layers: an on-VM daily database-freshness check (catches "VM was up, run failed or loaded nothing") and an **off-VM GitHub Actions watchdog** on blob-snapshot age (catches "the VM never powered on at all", which no on-VM cron can ever catch).

**Current state:** `new_portal_rto_etl.sh` exists but is **not in cron** and is not shaped for cron — see "What V1 Readers Get Wrong" below. `ops/production_vm.crontab` holds exactly three entries, all V1, all day 1, and the live crontab matches it byte-for-byte (verified 2026-11-03). No V2 dbt model exists. No new-portal runbook exists.

The live table, measured 2026-11-03, is itself the argument for this plan:

| Fact | Value |
|---|---|
| `fact_ev_data_by_rto_v2` rows | 2,364,454 |
| 2026 months covered | 1–9 only; **zero month-10 rows** |
| `MAX([date])` | September 2026 |
| Distinct `status_scope` | `ALL_STATUSES` only — the ACTIVE drop is confirmed in data |
| September 2026 captured | 2026-09-24 17:20–17:51, with six days of the month still unelapsed |
| September 2026 | 15,049 rows / 239,579 EV vs V1's settled 332,523 = **−27.95%** |
| 2026 months 1–8 vs V1 | +0.14% to +1.70% — the pipeline is healthy; only the in-flight month is short |

So the newest month in the table is a stub, October never ran, and nothing alerted. That combination is what every design decision below is aimed at.

**Tech Stack:** Python 3.12, `requests`, pyodbc (VM only, guarded import), Azure Blob Storage SDK (VM + GH Actions only, guarded import), POSIX sh / bash, cron, GitHub Actions, unittest.

**Spec:** No separate spec. `docs/new_portal_v2_design.md` carries the source-level decisions; this plan carries the scheduling design.

---

## What V1 Readers Get Wrong About V2

Read this before copying anything from `rto_ev_data_etl.sh`. Four differences are load-bearing:

1. **V2 takes a `YEAR`, not `MON YYYY`.** `new_portal_rto_etl.sh` is `bash new_portal_rto_etl.sh [YEAR]`, defaulting to `$(date '+%Y')`. It never calls `describe_run_args` and never calls `runtime_config.resolve_month_year_args` / `get_previous_month_year_label`. One portal call returns a whole year of months, so the unit of work is a year, and the pipeline re-pulls the year to date every run and replaces it wholesale (`REPLACEMENT_SCOPE_COLUMNS = ["year", "status_scope", "state_code"]`). "Last month" is not an input to V2 at all — it is only an *assertion* about the output.
2. **V2 never sets `RUN_LABEL`.** Because it skips `describe_run_args`, every Google Chat alert it sends reports `Run: unknown`. V1 sets `RUN_LABEL="$(describe_run_args "$@")"`.
3. **One fetch attempt is not a run.** `new_portal_rto_etl.sh` calls `python3 -m new_portal.rto_fetch --year Y` exactly once. On this portal a single attempt usually does *not* complete — availability is per-office and rotates over minutes. `rto_fetch` then exits non-zero (correctly), so on cron this script would mostly just alert and ingest nothing. The sweep loop in `ops/new_portal_sweep_until_complete.sh` is what actually completes a year.
   **Stronger than that, as of 2026-11-03: `new_portal_rto_etl.sh` cannot complete a nationwide year at all**, because its single attempt runs at the unsafe default pacing and will always be throttled out (see Global Constraints). Treat it as a single-state / single-smoke-test tool, not a "manual full run" tool.
4. **Never compose a sweep with `new_portal_rto_etl.sh`. The two cannot be chained.** `rto_fetch` **deletes the checkpoint on a clean completion** — `rto_fetch.py` ~1220: *"Every office finished cleanly. Drop the checkpoint so a later run for this year starts fresh"*, logging `Run complete with no gaps; removed ...__progress.jsonl`. The ETL script's **first** stage is `fetch`. So "sweep until complete, then run the ETL script" re-pulls the entire year from scratch, at the unsafe default rate, with no checkpoint to resume from — and `rto_fetch` **overwrites the complete CSV** with the resulting throttled partial.
   This is not hypothetical. Measured 2026-11-04: a paced sweep completed 2026 at 00:08:56 (`Wrote 169083 rows`, zero gaps, zero 429s, 1h29m); the ETL script invoked immediately after then re-fetched from zero for 87 minutes, overwrote the CSV with 123,693 rows and 314 gaps, and failed at `step=fetch` — costing the complete CSV and emitting a FAILED alert, while the database stayed correctly untouched.
   The consequence for this plan: `new_portal_rto_monthly.sh` must invoke the post-sweep stages **directly** (`rto_blob_snapshot` → `rto_bulk_ingest`, falling back to `rto_ingest`), exactly as `ops/new_portal_backfill_driver.sh` already does, and must never shell out to `new_portal_rto_etl.sh`. This is also a second, independent reason the gate must not trust the fetch's exit code: the CSV on disk at gate time need not be the CSV the successful sweep produced.
5. **There is no dbt stage, because there is no V2 curated model.** `climate_dot_dbt/models/curated/` contains only `rto_wise_ev_data`, `oem_wise_ev_data` and `state_wise_ev_data`. Copying V1's `dbt run --select ...` stage would fail on a model that does not exist. (Note: CLAUDE.md still says State does not run dbt — stale as of commit `50383ffd`; it does now. All three V1 entrypoints have a dbt stage; V2 must not.)

Also verified and worth one line: **`new_portal_rto_etl.sh` line 1 is `e#!/bin/bash`** — a stray `e` before the shebang, present in the committed file since `e3fa8518`. Under `bash script.sh` it is harmless noise (`e: command not found`, before `set -eE` arms), but executed directly the kernel finds no shebang. Do not copy line 1 from that file.

## Global Constraints

- **Never ingest a partial run.** Completeness is a property of the whole nationwide sweep, not of a month, because one call returns a whole year. The gate must be an automated blocking step, not a human eyeball, and it must be independent of the fetch's own exit code (see Task 1 for why that is not redundant).
- **`ALL_STATUSES` only.** `DEFAULT_STATUS_SCOPES = ["ALL_STATUSES"]`. `ACTIVE` is an as-of-today measure that decays and was deliberately dropped; never add `--status-scopes` to a scheduled run.
- **One session per worker, not more threads per session.** `DEFAULT_WORKERS = 8`, each with its own `NewPortalClient`; threads sharing one session gain almost nothing (the portal serializes per JSESSIONID). ~90,000–135,000 API calls per full run.
- **Pace the sweep below the portal's token bucket, or it cannot finish.** Measured 2026-11-03 on the VM: the portal serves roughly **1,500 requests per 60s (~25/s), then hard-429s for ~20s**, on a ~60s cycle. The shipped default (8 workers × `DEFAULT_SLEEP_SECONDS = 0.2`) runs at **26.7/s — just over the line**. Calibrated over 150s sustained: 0.2s → 1,126 × 429; `--sleep-seconds 0.30` → 24.0/s, zero 429s; `--sleep-seconds 0.45` → 16.5/s, zero 429s. **A scheduled run must pass `--sleep-seconds 0.45`** (~31% margin; 0.30 leaves only ~10%, and the probe exercised one endpoint while a real run mixes heavier ones).
  This is not a tuning nicety — it is fatal, because **nothing in `new_portal/` handles 429 specially.** `client.py` treats it like any other non-200, and `rto_fetch` sets `retry_attempts=1` deliberately (fail fast, defer to the recovery pass). Correct for a dead office; wrong for a throttle window, where each ~20s lockout burns ~200 offices as gaps and trips the `ABORT_WINDOW=200` / `ABORT_FAILURE_RATE=0.95` breaker. **Observed 2026-11-03: a 20-attempt sweep died at ~205 offices every single attempt, gaining ~10 complete offices each, and completed 517 of 1,676 in ~2 hours before giving up. Raising `MAX` buys nothing at the wrong rate.**
  Note the trap this sets for anyone re-measuring: a short burst always looks healthy. The 2026-08-19 "8 sessions ≈ 25.8 calls/s with zero 429s" figure was taken on ~76s of load, too short to drain the bucket — the number was right, but "safe to sustain" did not follow from it. Re-measure with ≥150s of sustained load.
- **Budget hours, not the 31-minute best case.** A complete 2026 sweep took **31 minutes** at the unsafe default rate (2026-09-24, 17:20:01 → 17:51:17). At the required `0.45` pacing the same work is ~16.5/s against ~90–135k calls, so budget **~2–2.5 hours** for a full year, plus the recovery pass. Scheduling must still allow for *several* sweeps, not one, because a single attempt usually does not complete.
- **Every run writes a partial, in-flight current month, by design.** A run on day 3 of December pulls Jan–Dec and the December rows cover three days. This is inherent to a year-at-a-time source and is not a defect — but it means `MAX([date])` is *always* the current month and is therefore **useless as a freshness signal**. See Task 2.
- **Blob snapshots are immutable.** `rto_blob_snapshot.py` uploads with `overwrite=False` and the blob name carries the CSV's mtime. Never overwrite a previous run's snapshot; it is the only record of what we believed at a given moment, and it is the rollback source for ingestion.
- **An outage must not silently produce a partial month.** A portal outage looks different from being blocked: a 503 "No server is available to handle this request" from `https://analytics.parivahan.gov.in/analytics/` is portal-side. `rto_fetch`'s preflight turns that into one failed request and a clear message; its circuit breaker (`ABORT_WINDOW=200`, `ABORT_FAILURE_RATE=0.95`) stops a run against a dead portal. The *schedule* must add tolerance on top: a second scheduled attempt on a later day that resumes from the checkpoint.
- **Secrets live only in `config.yaml` on the VM** (gitignored). Read them via `runtime_config.load_config()`. The GH Actions watchdog is the one exception and must use a repository secret, never a committed value.
- **The VM is off by default** and is started/stopped by external automation not represented in this repo. A cron entry does nothing if the VM is not powered on. This is a hard external dependency, not something any task here can satisfy.
- **No V2 dbt stage.** Do not add one; do not create a curated V2 model as part of this work.
- **Do not modify a script cron already calls.** `rto_ev_data_etl.sh`, `oem_data_etl.sh`, `state_ev_data_etl.sh` and `ops/etl_runtime.sh` are untouched by this plan. `new_portal_rto_etl.sh` is also left alone — it stays the manual single-pass tool; cron gets a new file.
- **CI import safety.** CI installs only `requirements-ci.txt` (PyYAML, openpyxl, pandas, requests, selenium). Any module a unit test imports must import cleanly **without `azure` or `pyodbc`**, using the guarded-import pattern from `blob_storage_utils.py`. Every new test module must be added to `UNIT_TEST_MODULES` in `ops/run_repo_checks.sh` or it never runs.

---

## File Structure

### Create

| Path | Single responsibility |
|---|---|
| `new_portal/rto_completeness.py` | Decide, from artifacts on disk, whether one year's fetch output is safe to ingest. Pure functions + a CLI that exits non-zero when it is not. No DB, no azure. |
| `tests/test_new_portal_rto_completeness.py` | Tests for the above. |
| `ops/check_pipeline_freshness.py` | On-VM: read `MAX([inserted_at])` from a fact table as the staleness signal, plus a secondary target-month-settled assertion; alert via `ops/send_chat_alert.py`. Guarded `pyodbc` import. |
| `tests/test_pipeline_freshness.py` | Tests for both predicates and the alert invocation, including the `MAX([date])`-masking regression. |
| `ops/truncate_v2_scratch_tables.py` | Clear `landing_`/`staging_fact_ev_data_by_rto_v2` after a successful run, so manual recovery never finds a stale year sitting in staging. Guarded `pyodbc` import. |
| `tests/test_v2_scratch_cleanup.py` | Tests that it truncates only the scratch tables and never the fact table. |
| `ops/check_snapshot_freshness.py` | Off-VM: age of the newest blob snapshot in the V2 CSV container, as a dead-man's switch. Guarded `azure` import; pure age/verdict logic separated out. |
| `tests/test_snapshot_freshness.py` | Tests for the age/verdict logic, with a fake container client. |
| `new_portal_rto_monthly.sh` | The cron entrypoint: sweep → **gate** → snapshot → bulk ingest (row-by-row fallback) → post-ingest verify, under `etl_runtime.sh` alerting with `RUN_LABEL` set. |
| `tests/test_new_portal_monthly_entrypoint.py` | Text-level invariants on the entrypoint: stage order, gate present and before snapshot, `RUN_LABEL` set, no `--allow-gaps`, no `--limit-rtos`, no `--status-scopes`. |
| `.github/workflows/freshness_watchdog.yml` | Scheduled off-VM workflow that runs `ops/check_snapshot_freshness.py`. The only thing here that survives the VM being off. |
| `docs/runbooks/new_portal_rto.md` | Runbook: what the schedule is, what each alert means, how to re-run, how to roll back. |

### Modify

| Path | Change |
|---|---|
| `ops/production_vm.crontab` | Add the V2 primary run, the V2 retry run and the daily on-VM freshness check. Document the required VM uptime window. |
| `ops/run_repo_checks.sh` | Add the four new test modules to `UNIT_TEST_MODULES`; add `new_portal_rto_monthly.sh` (and `new_portal_rto_etl.sh`) to the `bash -n` list. |

### Explicitly not modified

`new_portal_rto_etl.sh`, `ops/etl_runtime.sh`, `ops/send_chat_alert.py`, `ops/new_portal_sweep_until_complete.sh`, `ops/new_portal_backfill_driver.sh`, `new_portal/rto_fetch.py`, `new_portal/rto_ingest.py`, `new_portal/rto_bulk_ingest.py`, any V1 entrypoint, any dbt model.

**Why not `new_portal_backfill_driver.sh`:** it is a *backfill* driver. It takes a `YEARS` list and **skips any year that already has rows** in `fact_ev_data_by_rto_v2`. For a monthly run the target year always already has rows, so it would skip every time. Its sweep→snapshot→ingest→verify shape is the right shape and should be borrowed; the script itself is the wrong tool.

**Why `new_portal_sweep_until_complete.sh` is the right inner loop:** it resumes from the per-year checkpoint, visits unvisited offices before gapped ones, stops early on stall, and exits 0 **only** when `rto_fetch` itself exits 0 (which already means zero gaps and zero failures). Call it, do not reimplement it.

**But it cannot pass the required pacing flag, and must be given one.** The script hardcodes its fetch line:

```bash
./venv/bin/python -u -m new_portal.rto_fetch --year "$YEAR" > "$LOG" 2>&1
```

There is no way to add `--sleep-seconds 0.45` from outside, and without it a scheduled sweep cannot complete the year (see Global Constraints). So **Task 4 gains a prerequisite**: add a pass-through to this script — a `SLEEP_SECONDS` env var with the current behaviour as its default — rather than reimplementing the loop or forking a copy. It is **not** a script cron already calls, so editing it does not violate the "do not modify a script cron already calls" constraint; it is in the `ops/` support layer and the V1 entrypoints do not touch it. Make it the smallest possible change:

```bash
SLEEP_SECONDS=${SLEEP_SECONDS:-0.45}
...
./venv/bin/python -u -m new_portal.rto_fetch --year "$YEAR" \
  --sleep-seconds "$SLEEP_SECONDS" > "$LOG" 2>&1
```

Defaulting to `0.45` rather than `rto_fetch`'s own `0.2` is deliberate: this script exists only to run *nationwide, multi-hour* sweeps, which is exactly the case the bucket punishes. The 0.2 default stays correct for the short, targeted runs `rto_fetch` is invoked for directly.

---

## Task 1: Independent completeness gate

**Files:** create `new_portal/rto_completeness.py`, create `tests/test_new_portal_rto_completeness.py`, modify `ops/run_repo_checks.sh`.

**Interfaces**

*Consumes (existing, do not change):*
- `new_portal.rto_fetch.build_output_path(year, output_dir=None) -> Path`
- `new_portal.rto_fetch.build_gap_manifest_path(year, output_dir=None) -> Path`
- `new_portal.rto_fetch.build_progress_path(year, output_dir=None) -> Path`
- `new_portal.rto_fetch.load_progress(path) -> dict[str, tuple[list[dict], list[Gap]]]`
- `new_portal.rto_fetch.office_is_complete(entry) -> bool`
- `new_portal.rto_fetch.load_rto_targets(states=None) -> list[dict]`
- `new_portal.schema.STATUS_SCOPE_ALL`, `new_portal.schema.CSV_COLUMNS`

*Produces (for Task 4):*
- `CompletenessReport` dataclass: `expected_offices: int`, `csv_offices: int`, `csv_rows: int`, `months_present: set[int]`, `gap_manifest_present: bool`, `csv_fetched_at: datetime`, `stale_csv: bool`, `failures: list[str]`
- `summarize_csv(csv_path: Path) -> CsvSummary` — offices, rows, months, distinct `status_scope` values
- `verify_run_complete(year: int, target_month: int, *, csv_path: Path | None = None, gap_manifest_path: Path | None = None, expected_office_count: int | None = None, min_fetched_at: datetime | None = None) -> CompletenessReport`
- `main(argv: list[str] | None = None) -> int` — CLI `python3 -m new_portal.rto_completeness --year Y --target-month M [--min-fetched-at EPOCH]`, returns `0` when `report.failures` is empty, `1` otherwise, and logs every failure.

### Why this is not redundant with `rto_fetch`'s exit code

The sweep's exit code says "this invocation finished with no gaps". It does **not** say the CSV on disk is that invocation's output, nor that the run covered the whole country, nor that it covers the month we care about. Four real holes it leaves, all of which this gate closes:

- The CSV path for a year is fixed, so a stale CSV from an earlier failed run sits at exactly the path `rto_blob_snapshot` and `rto_ingest` read. Nothing currently proves provenance. → `min_fetched_at` vs the CSV's mtime.
- A checkpoint built by a `--states MH` run makes "all offices complete" true for 48 offices. → `expected_office_count` from `load_rto_targets()` with no state filter.
- `--allow-gaps` makes `rto_fetch` exit 0 *with* gaps. → `gap_manifest_present`.
- A sweep can complete a year that has no rows at all for the month we are scheduling for. → `target_month in months_present`.

It also must *not* over-assert, in two directions:

- An office that genuinely reported nothing all year is legitimate and contributes no CSV rows (the first 13 offices in the national list are Andaman islands with no 2026 registrations). So the office count is checked against the **checkpoint**, where an empty office is still recorded and still `office_is_complete`, and the CSV check is on aggregate month coverage, never per-office month coverage.
- **The newest month in the CSV is expected to be partial** — a run on day 3 of December includes three days of December. The gate therefore asserts on `target_month`, the *previous* and fully-elapsed calendar month, and must **never** assert completeness of the in-flight current month. This is where the gate and the schedule fit together: because the run is on day 3 and the target is a closed month, the target is never in flight. The September 2026 stub in the live table arose precisely because a run on day 24 captured the month it was itself inside — something a day-3 schedule targeting the previous month cannot produce.

### Steps

1. - [ ] Write `tests/test_new_portal_rto_completeness.py` with these failing tests, using a tmpdir and hand-written CSV/JSONL fixtures (no network, no DB):
   - `test_clean_run_has_no_failures` — complete checkpoint for every target, CSV with the target month, no manifest → `report.failures == []`
   - `test_gap_manifest_present_is_a_failure` — asserts the failure string mentions the manifest path
   - `test_partial_state_checkpoint_is_a_failure` — checkpoint covers 48 of the expected offices → failure naming both counts
   - `test_csv_older_than_min_fetched_at_is_a_failure` — asserts `report.stale_csv is True`
   - `test_target_month_absent_from_csv_is_a_failure`
   - `test_unexpected_status_scope_in_csv_is_a_failure` — any value other than `ALL_STATUSES`
   - `test_office_with_no_rows_but_complete_checkpoint_is_not_a_failure` — the Andaman case
   - `test_partial_in_flight_current_month_is_not_a_failure` — a CSV whose newest month is a 3-day stub must pass, provided `target_month` itself is present. Asserts the gate does not mistake expected partiality for incompleteness.
   - `test_main_returns_one_on_any_failure` / `test_main_returns_zero_on_clean_run`
2. - [ ] Run `python3 -m unittest tests.test_new_portal_rto_completeness` and confirm it fails on import (module does not exist).
3. - [ ] Implement `new_portal/rto_completeness.py` with the signatures in the Interfaces block. Import only from `new_portal.rto_fetch`, `new_portal.schema`, stdlib and `csv`. No `azure`, no `pyodbc`, no `pandas` — keep it importable in CI with nothing but `requirements-ci.txt`.
4. - [ ] Add `tests.test_new_portal_rto_completeness` to `UNIT_TEST_MODULES` in `ops/run_repo_checks.sh`.
5. - [ ] Run `PYTHON_BIN=<ci-venv>/bin/python bash ops/run_repo_checks.sh` in a venv built from `requirements-ci.txt` only; confirm green.
6. - [ ] Commit.

**Rollback:** none — new file, no DB, no crontab.

---

## Task 2: On-VM load-recency check

**Files:** create `ops/check_pipeline_freshness.py`, create `tests/test_pipeline_freshness.py`, modify `ops/run_repo_checks.sh`.

### Why `MAX([date])` cannot detect a missed run, and must not be the primary signal

This was a bug in the first draft of this plan. It is written out in full because the wrong version looks obviously right.

A V2 run pulls a whole year, so **every run writes rows for the in-flight current month**. A December 3 run leaves `MAX([date])` = December. If the January 3 run then fails completely, `MAX([date])` is *still* December, "previous calendar month" is *also* December, a `MAX([date])`-based check compares them equal and reports **FRESH** — while the newest month in the table is a 3-day stub and the pipeline has been dead for a month.

That is not hypothetical. It is the table's state right now: `MAX([date])` = September 2026, captured 2026-09-24 17:20-17:51 with six days of the month still unelapsed, 15,049 rows / 239,579 EV against V1's settled 332,523 (**-27.95%**), and no October rows at all. Months 1-8 of 2026 sit at +0.14% to +1.70% against V1, so the pipeline itself is healthy; only the month that was in flight at capture time is short. A `MAX([date])` check would have reported healthy throughout.

**Key the check on load recency, not data recency.** `fact_ev_data_by_rto_v2.[inserted_at] DATETIME NOT NULL DEFAULT GETDATE()` is the final column of the table (confirmed in `sql/migrations/2026-08-18_new_portal_rto_v2_tables.sql`; `rto_bulk_ingest` sets it explicitly with `GETDATE()` on the staging transfer). `MAX([inserted_at])` answers "did a run land recently", which is the actual question. `MAX([date])` is kept only as a **secondary** assertion, never as the staleness signal, and the first draft's `--grace-days 6` is **dropped entirely** — it was sized against the wrong column and means nothing against `inserted_at`.

### The queries

```sql
-- PRIMARY: did a run land at all? This is the staleness signal.
SELECT MAX([inserted_at]) AS last_loaded_at
FROM dbo.fact_ev_data_by_rto_v2
WHERE [status_scope] = ?;

-- SECONDARY: did the target month settle, or is it another in-flight stub?
-- Compares the target month (previous calendar month, fully elapsed) against
-- the month before it, which is known-settled.
SELECT [year], [month], COUNT(*) AS row_count, SUM([total]) AS total_registrations
FROM dbo.fact_ev_data_by_rto_v2
WHERE [status_scope] = ? AND [year] = ? AND [month] IN (?, ?)
GROUP BY [year], [month]
ORDER BY [year], [month];
```

`[date]` here is a real `DATE` column, unlike V1's `nvarchar` DD/MM/YYYY text which needs `CONVERT(date, [date], 103)`. Do not copy a V1 date cast into this module. Grouping on `[year], [month]` rather than `[date]` keeps the December-to-January boundary a parameter concern rather than a SQL one.

### Thresholds, derived not guessed

- **`--max-load-age-days 35`.** The widest *legitimate* gap between two successful loads under the Task 7 schedule is: last success on day 3 of month N (primary run), next success on day 5 of month N+1 (the retry, after a failed day 3). For a 31-day month that is 31 - 3 + 5 = **33 days**. The daily check runs at 06:15 UTC and a run finishes by ~04:00, so ~33 days is the greatest age any check can legitimately observe. 35 gives a two-day buffer and nothing more. **If the retry day moves, this number moves with it.**
- **`--min-month-total-ratio 0.85`**, computed from `SUM(CAST([total] AS bigint))` — **not** from `COUNT(*)`.

  **Row count cannot detect a stub month at any threshold.** Measured on 2026, `ALL_STATUSES`:

  | mo | rows | row_ratio | total | tot_ratio | ev | ev_ratio |
  |---|---|---|---|---|---|---|
  | 1 | 15,854 | — | 2,845,484 | — | 229,080 | — |
  | 2 | 16,161 | 1.0194 | 2,526,268 | 0.8878 | 203,765 | 0.8895 |
  | 3 | 16,130 | 0.9981 | 2,819,910 | 1.1162 | 292,018 | 1.4331 |
  | 4 | 16,179 | 1.0030 | 2,725,660 | 0.9666 | 251,587 | 0.8615 |
  | 5 | 15,884 | 0.9818 | 2,577,306 | 0.9456 | 276,554 | 1.0992 |
  | 6 | 16,162 | 1.0175 | 2,603,196 | 1.0100 | 310,590 | 1.1231 |
  | 7 | 16,502 | 1.0210 | 2,632,295 | 1.0112 | 332,523 | 1.0706 |
  | 8 | 16,261 | 0.9854 | 2,452,785 | 0.9318 | 301,937 | 0.9080 |
  | **9** | **15,049** | **0.9255** | **1,904,174** | **0.7763** | **239,579** | **0.7935** |

  Healthy months sit at 0.9818–1.0210 on row count and the September stub is **0.9255 — inside that band's neighbourhood and well above any usable threshold**. A partial-month capture keeps full *breadth* (36 states, 258 offices, every office reporting) and shrinks only the counts *inside* each row; row count measures breadth, which is exactly what a stub does not lose. So no threshold on `COUNT(*)` separates the two, and the first draft's 0.85 would have **passed** the stub.

  `SUM([total])` separates: healthy 0.8878–1.1162, stub 0.7763.

  **EV sum must not be used**, even though it also catches the stub (0.7935): its healthy range is 0.8615–1.4331, so month 4's perfectly healthy 0.8615 is already at the stub's doorstep and it would false-alarm.

  **The 0.85 margin is thin, and should not be presented as comfortable.** The healthy minimum observed is 0.8878 and the stub is 0.7763 — a 1.1-point gap, with 0.85 sitting roughly 3.8 points above the stub and 3.8 below the healthiest-worst month. It rests on **seven month-over-month samples from a single year**, and a genuinely weak trading month could trip it. A ratio test that fires spuriously is worse than none, because it trains the operator to ignore the channel.

- **`COUNT(*)` stays in the query, as reported context in the alert body only — never a pass/fail signal.** September is the worked example: an operator reading *"rows 15,049 vs 16,261 (ratio 0.93), total 1,904,174 vs 2,452,785 (ratio 0.78)"* can see instantly which signal moved and why the verdict fired. Label it explicitly as context in the alert text so nobody later promotes it to a threshold.

### Interfaces

*Consumes:* `runtime_config.load_config()`, `sqlserver_utils.connect_with_retry`, `ops/send_chat_alert.py` (invoked as a subprocess, exactly as `etl_runtime.sh` does — do not import it, so the alert path stays identical to the one production already exercises).

*Produces (for Task 4 and Task 7's crontab entry):*
- `fetch_last_loaded_at(connection, table_name: str, *, status_scope: str) -> datetime | None`
- `fetch_month_row_counts(connection, table_name: str, *, status_scope: str, year: int, months: tuple[int, int]) -> dict[int, MonthStats]` — `MonthStats` carries `row_count: int`, `total_registrations: int | None`
- `evaluate_load_recency(last_loaded_at: datetime | None, reference: datetime, *, max_load_age_days: int) -> FreshnessVerdict` — `is_stale: bool`, `age_days: float | None`, `message: str`
- `evaluate_month_settled(target: MonthStats | None, preceding: MonthStats | None, *, min_month_total_ratio: float) -> FreshnessVerdict` — compares `total_registrations`, never `row_count`. The verdict's `message` must carry both ratios, with the row ratio labelled as context.
- `target_month(reference_date: date) -> tuple[int, int]` — the previous calendar month, which is always fully elapsed. **No grace period:** the target is a closed month, so there is nothing to wait for.
- `main(argv) -> int` — CLI `--table fact_ev_data_by_rto_v2 --status-scope ALL_STATUSES --max-load-age-days 35 --min-month-total-ratio 0.85 --pipeline new_portal_rto_freshness`. Exits `1` and sends one `FAILED` alert if **either** verdict is stale, naming which; exits `0` and sends nothing when both pass.

The alert must distinguish the two causes in words, because the operator response differs: *load-recency stale* means no run has landed and the schedule or the VM is the problem; *month-not-settled* means a run landed but captured an in-flight month, so re-run now that the month is closed.

### Steps

1. - [ ] Write `tests/test_pipeline_freshness.py`:
   - `test_recent_load_is_not_stale` — loaded 4 days ago, limit 35
   - `test_load_older_than_limit_is_stale` — loaded 40 days ago, `is_stale is True`, message carries the age
   - `test_never_loaded_is_stale` — `last_loaded_at is None`
   - `test_thirty_three_day_gap_is_not_stale` — the legitimate day-3-to-day-5-next-month worst case must not alert
   - **`test_max_date_equal_to_previous_month_does_not_mask_a_missed_run`** — the regression test for this task's bug. A table whose `MAX([date])` is December but whose `MAX([inserted_at])` is 40 days old must be reported stale.
   - `test_target_month_is_previous_calendar_month`, and `test_target_month_crosses_the_year_boundary` for January to (prev year, 12)
   - `test_month_with_full_total_is_settled` — total ratio 0.98
   - **`test_stub_month_with_healthy_row_count_is_not_settled`** — the September 2026 numbers exactly: target `row_count=15,049, total=1,904,174` against preceding `row_count=16,261, total=2,452,785`. Row ratio 0.9255 looks fine, total ratio 0.7763 does not. This is the test that pins the metric choice; if someone switches the comparison back to `COUNT(*)`, only this test fails.
   - `test_verdict_message_reports_row_ratio_as_context` — asserts both ratios appear and that the row ratio is labelled as context, not as the reason
   - `test_three_day_stub_month_is_not_settled` — the September 2026 shape: 15,049 target rows against a preceding month several times larger
   - `test_missing_target_month_is_not_settled` — the October 2026 case: no rows at all
   - `test_main_sends_one_alert_naming_which_check_failed` — patch `subprocess.run`, assert `--status FAILED`, `--pipeline new_portal_rto_freshness`, and that the argv distinguishes load-recency from month-settled
   - `test_main_sends_no_alert_when_both_checks_pass`
   - `test_module_imports_without_pyodbc` — patch `sys.modules["pyodbc"] = None` and re-import; asserts the guarded import holds
2. - [ ] Run the test module and confirm failure.
3. - [ ] Implement `ops/check_pipeline_freshness.py`. Guard `import pyodbc` with `try/except ImportError: pyodbc = None` and raise a clear runtime error only when a connection is actually attempted.
4. - [ ] Add `tests.test_pipeline_freshness` to `UNIT_TEST_MODULES`.
5. - [ ] Run the CI-venv repo checks; confirm green.
6. - [ ] Commit.

**Rollback:** the script only reads (`SELECT MAX(...)`, `SELECT ... GROUP BY`). Nothing to undo. If the ratio check proves noisy, lower `--min-month-total-ratio` or disable it — but never weaken `--max-load-age-days` to silence an alert without recording why in the runbook; that is the one check standing between a dead pipeline and silence.

---

## Task 3: Off-VM snapshot watchdog (the dead-man's switch)

**Files:** create `ops/check_snapshot_freshness.py`, create `tests/test_snapshot_freshness.py`, create `.github/workflows/freshness_watchdog.yml`, modify `ops/run_repo_checks.sh`.

**Why blob age and not the database:** the whole point of this layer is that it runs somewhere the VM's power state cannot reach. A GitHub-hosted runner can reach Azure Blob Storage with a connection string. Whether it can reach the Azure SQL server depends on that server's firewall allow-list, which **this plan could not verify** — see Still Open. Blob snapshot age is a sound proxy regardless: `new_portal_rto_monthly.sh` snapshots *before* ingesting, so a fresh snapshot proves a run happened and completed its fetch, and its absence proves one did not.

**Container name — hard-code it, and understand the trap.** Verified 2026-11-03: the VM's `config.yaml` has **no `new_portal` storage key at all** (the block is `connection_string`, `container_name`, `csv_container_name`, `key`, `rto_wise_container_name`, `rto_wise_csv_container_name`, `state_wise_container_name`, `state_wise_csv_container_name`). Production therefore runs on `rto_blob_snapshot.resolve_container_name`'s fallback, `DEFAULT_CONTAINER_NAME = "new-portal-rto-csv"`. The watchdog cannot read `config.yaml` off the VM, so it must carry that name literally — but it must carry a comment saying the name is a **code default, not a configured value**, because the day someone adds `storage.new_portal_rto_csv_container_name` to `config.yaml`, the pipeline silently starts writing somewhere else and this watchdog keeps happily reading an abandoned container that never goes stale in the way it is watching for. That is a watchdog that reports healthy forever. Mitigation: import `DEFAULT_CONTAINER_NAME` from `new_portal.rto_blob_snapshot` rather than re-spelling the string, so a change to the default at least travels; and have the watchdog alert if the container is missing entirely rather than treating it as merely empty.

**Interfaces**

*Consumes:* `azure.storage.blob.BlobServiceClient` (guarded), `new_portal.rto_blob_snapshot.DEFAULT_CONTAINER_NAME` (imported, never re-spelled; `resolve_container_name` is not usable off the VM since there is no config to resolve against), `new_portal.rto_blob_snapshot.build_snapshot_blob_name` (for the name format only).

*Produces:*
- `newest_snapshot_age_days(blob_names_with_times: Iterable[tuple[str, datetime]], reference: datetime) -> float | None`
- `evaluate_snapshot_freshness(age_days: float | None, *, max_age_days: int) -> WatchdogVerdict` — `is_stale: bool`, `message: str`
- `main(argv) -> int` — `--max-age-days 40`, exits `1` and posts to Google Chat when stale.

The alert must say, in words, that **this check runs off the VM and a failure may mean the VM never started** — that is the sentence that would have shortened the Nov 1 incident from two days to one.

### Steps

1. - [ ] Write `tests/test_snapshot_freshness.py`:
   - `test_newest_snapshot_age_uses_the_newest_blob` — three blobs, asserts the newest wins
   - `test_no_blobs_returns_none`
   - `test_age_within_limit_is_not_stale` — 12 days, limit 40
   - `test_age_beyond_limit_is_stale` — 45 days, limit 40
   - `test_missing_container_is_stale_not_an_error` — an empty/absent container must alert, never crash silently
   - `test_stale_message_mentions_the_vm_may_not_have_started`
   - `test_module_imports_without_azure` — patch `sys.modules`, re-import, assert no `ImportError`
2. - [ ] Run and confirm failure.
3. - [ ] Implement `ops/check_snapshot_freshness.py` with `BlobServiceClient` behind a guarded import, listing blobs via a `container_client` the tests can substitute.
4. - [ ] Add `tests.test_snapshot_freshness` to `UNIT_TEST_MODULES`; run the CI-venv repo checks.
5. - [ ] Create `.github/workflows/freshness_watchdog.yml`: `on.schedule: - cron: "0 6 * * *"` plus `workflow_dispatch`; installs `azure-storage-blob` only; runs `python ops/check_snapshot_freshness.py --max-age-days 40`; passes `AZURE_STORAGE_CONNECTION_STRING` and `GOOGLE_CHAT_WEBHOOK_URL` from repository secrets. It must **not** read `config.yaml` — that file does not exist off the VM.
6. - [ ] Trigger it once via `workflow_dispatch` and confirm it reports *fresh* against the real container before trusting it. A watchdog that has never been seen to pass is not a watchdog.
7. - [ ] Commit.

**Rollback:** delete the workflow file, or disable it in the Actions UI. It performs no writes. Rotate the two repository secrets if the workflow is removed for good.

---

## Task 4: The cron entrypoint

**Files:** create `new_portal_rto_monthly.sh`, create `tests/test_new_portal_monthly_entrypoint.py`, modify `ops/run_repo_checks.sh`.

**Interfaces**

*Consumes:* `ops/etl_runtime.sh` (`describe_run_args`, `install_failure_alert_trap`, `send_success_alert`), `ops/new_portal_sweep_until_complete.sh`, `python3 -m new_portal.rto_completeness` (Task 1), `python3 -m new_portal.rto_blob_snapshot`, `python3 -m new_portal.rto_bulk_ingest`, `python3 -m new_portal.rto_ingest`, `ops/check_pipeline_freshness.py` (Task 2).

*Produces (for Task 7):* the script path, and `RUN_LABEL`/`CURRENT_STEP` values that appear in alerts.

**Required shape** (the stage order is the contract; everything else is detail):

```
set -eE
PROJECT_ROOT=/home/climate_dot_data/DataScraper_VahanParivahan
. venv/bin/activate ; . ops/etl_runtime.sh ; cd "$PROJECT_ROOT"
TARGET_YEAR / TARGET_MONTH   # default: previous calendar month's year and month
RUN_START_EPOCH="$(date -u +%s)"
RUN_LABEL="$(describe_run_args "$@")"        # fixes the "Run: unknown" alert gap
ETL_PIPELINE_NAME="new_portal_rto"
ETL_LOG_FILE=.../new_portal_rto_monthly_logs.txt
install_failure_alert_trap

CURRENT_STEP="fetch_sweep"       YEAR=$TARGET_YEAR SLEEP_SECONDS=0.45 \
                                 MAX=30 WAIT=300 STALL_LIMIT=6 \
                                 bash ops/new_portal_sweep_until_complete.sh
CURRENT_STEP="completeness_gate" python3 -m new_portal.rto_completeness \
                                   --year "$TARGET_YEAR" --target-month "$TARGET_MONTH" \
                                   --min-fetched-at "$RUN_START_EPOCH"
CURRENT_STEP="blob_snapshot"     python3 -m new_portal.rto_blob_snapshot --year "$TARGET_YEAR"
CURRENT_STEP="ingestion"         python3 -m new_portal.rto_bulk_ingest --year "$TARGET_YEAR" \
                                   || python3 -m new_portal.rto_ingest --year "$TARGET_YEAR"
CURRENT_STEP="post_ingest_check" python3 ops/check_pipeline_freshness.py \
                                   --table fact_ev_data_by_rto_v2 \
                                   --status-scope ALL_STATUSES \
                                   --max-load-age-days 35 \
                                   --min-month-total-ratio 0.85 \
                                   --pipeline new_portal_rto_freshness
CURRENT_STEP="staging_cleanup"   python3 ops/truncate_v2_scratch_tables.py   # Task 5
CURRENT_STEP="completed"         send_success_alert
# no dbt stage — see Global Constraints
```

Decisions the implementer should not re-litigate:

- **The gate sits between the sweep and the snapshot.** Snapshotting a partial CSV is not harmful on its own (snapshots are immutable and a partial one is honest history), but snapshot-then-gate would leave a partial snapshot as the newest blob, which would then satisfy Task 3's watchdog and mask a failure. Gate first.
- **`--min-fetched-at "$RUN_START_EPOCH"`** is what makes the gate reject a stale CSV left by an earlier failed run.
- **Bulk ingest with a row-by-row fallback**, borrowed from `new_portal_backfill_driver.sh`: an expired SAS or a permissions change should cost time, not a month of data.
- **The sweep's env knobs get explicit values** (`SLEEP_SECONDS`, `MAX`, `WAIT`, `STALL_LIMIT`) rather than inheriting defaults, so the maximum wall-clock cost of a cron run is bounded and reviewable. Use `SLEEP_SECONDS=0.45 MAX=30 WAIT=300 STALL_LIMIT=6` and record the reasoning in the runbook, not only in the script.
  `SLEEP_SECONDS=0.45` is the measured-safe pacing (see Global Constraints) and is **not** optional. `MAX` drops from the first draft's 36 to 30 because the arithmetic changed: at `0.45` pacing a clean sweep is ~2–2.5 hours of *fetching*, so the binding cost is the sweeps themselves, not the `WAIT` between them. 30 attempts x 300s of waiting is 2.5h of idle on top of that — a worst case near 5 hours, which is the real ceiling to review. Do not raise `MAX` to paper over 429s: at the wrong rate every attempt dies at ~205 offices regardless of how many you allow (measured 2026-11-03).
- **Never pass `--allow-gaps`, `--limit-rtos`, `--states` or `--status-scopes`** from this script. Each of them makes "complete" mean something narrower than the gate assumes.

### Steps

1. - [ ] Write `tests/test_new_portal_monthly_entrypoint.py` — reads the script text and asserts invariants (cheap, but it is what stops a future edit from quietly reordering the gate):
   - `test_stages_appear_in_required_order` — `fetch_sweep` < `completeness_gate` < `blob_snapshot` < `ingestion`
   - `test_gate_precedes_snapshot_and_ingestion`
   - `test_sets_run_label_from_describe_run_args`
   - `test_installs_failure_alert_trap_and_sends_success_alert`
   - `test_never_passes_allow_gaps_or_limit_rtos_or_status_scopes`
   - `test_has_no_dbt_stage` — asserts `"dbt run"` is absent
   - `test_shebang_is_the_first_bytes_of_the_file` — guards against the `e#!/bin/bash` defect in the sibling script
   - `test_passes_min_fetched_at_to_the_gate`
   - `test_post_ingest_check_runs_after_ingestion_and_before_success_alert`
   - `test_staging_cleanup_is_the_last_stage_before_the_success_alert`
2. - [ ] Run and confirm failure (file does not exist).
3. - [ ] Write `new_portal_rto_monthly.sh` to the shape above. Derive `TARGET_YEAR`/`TARGET_MONTH` from the previous calendar month, with optional `YYYY MM` positional override so a missed month can be re-run by hand.
4. - [ ] Add `new_portal_rto_monthly.sh` and `new_portal_rto_etl.sh` to the `bash -n` list in `run_shell_checks`, and `tests.test_new_portal_monthly_entrypoint` to `UNIT_TEST_MODULES`.
5. - [ ] Run the CI-venv repo checks; confirm green.
6. - [ ] **Smoke test on the VM** (ask the maintainer to power it on): `bash new_portal_rto_monthly.sh` with the sweep pointed at one small state via a temporary local edit, or — better, because it changes no code — run the four stages by hand in order and confirm the gate *rejects* the deliberately-partial result. The gate refusing to ingest is the single most important behaviour here and it cannot be proven by a unit test.
7. - [ ] Commit.

**Rollback:** the script is new and not yet in cron, so rollback is "do not install the crontab line" (Task 7). If a run has already ingested, see Task 6's database rollback.

---

## Task 5: Clear the stale scratch tables after a successful run

**Files:** create `ops/truncate_v2_scratch_tables.py`, create `tests/test_v2_scratch_cleanup.py`, modify `ops/run_repo_checks.sh`.

### What is actually wrong, stated precisely

Measured 2026-11-03: `landing_fact_ev_data_by_rto_v2` and `staging_fact_ev_data_by_rto_v2` both hold **160,573 rows of 2013 data** (ym 201301..201312, 36 states), left behind by an old backfill and never cleared.

**This is not an ingestion-correctness bug, and the plan should not pretend it is.** Verified in code: `new_portal/rto_bulk_ingest.py` executes `TRUNCATE TABLE dbo.landing_...` *before* its `BULK INSERT` and `TRUNCATE TABLE staging_...` *before* the staging transfer; the row-by-row path in `etl_ingestion.py` likewise truncates staging before loading. Both paths are self-protecting, so a monthly run does **not** inherit the stale year into the fact table. Nothing is currently being double-counted.

What is genuinely wrong is that nothing truncates at the *end*, so both tables permanently retain a full copy of whatever year was loaded last. Two real consequences:

1. **It is a loaded gun pointed at this plan's own rollback procedures.** `rto_bulk_ingest.build_delete_query()` deletes from the fact table where a matching row EXISTS *in staging*, scoped on `(year, status_scope, state_code)`. In the normal flow staging was just repopulated, so that is correct. But Task 6's and Task 7's rollback steps are **manual recovery sessions**, and a human or agent who runs `build_delete_query()`, or `INSERT INTO final SELECT * FROM staging`, by hand against the current contents of staging would operate on **2013** — deleting and re-inserting a year nobody intended to touch. That is exactly the shape of the orphaned-row and duplicate-data incidents in CLAUDE.md's August 2026 remediation.
2. **It misleads anyone inspecting state.** Querying staging to see "what did the last run load" returns 2013, which reads as a far more alarming problem than it is, and costs an investigation.

So: truncate at the end of a successful run, and make the *reason* the foot-gun, not a fabricated correctness claim.

### Interfaces

*Consumes:* `runtime_config.load_config()`, `sqlserver_utils.connect_with_retry`, `new_portal.rto_bulk_ingest.LANDING_TABLE`, `new_portal.rto_ingest.STAGING_TABLE` (import the names; never re-spell them).

*Produces (for Task 4's `staging_cleanup` stage):*
- `SCRATCH_TABLES: tuple[str, ...]` — `(LANDING_TABLE, STAGING_TABLE)`, derived from the modules above
- `truncate_scratch_tables(connection, *, tables: Iterable[str] = SCRATCH_TABLES) -> dict[str, int]` — returns rows removed per table, read with `COUNT(*)` before truncating so the log records what was cleared
- `main(argv) -> int` — CLI with `--dry-run` (counts and logs, truncates nothing). Exit non-zero only on a database error; an already-empty table is success.

### Steps

1. - [ ] Write `tests/test_v2_scratch_cleanup.py` against a fake connection/cursor:
   - `test_scratch_tables_are_the_landing_and_staging_tables` — asserts the names come from the ingest modules, not string literals
   - `test_truncate_issues_one_truncate_per_table`
   - `test_truncate_counts_rows_before_removing_them` — asserts the `COUNT(*)` precedes the `TRUNCATE` for each table
   - `test_dry_run_issues_no_truncate`
   - `test_already_empty_table_is_success_not_an_error`
   - `test_never_touches_the_fact_table` — asserts no issued statement mentions `fact_ev_data_by_rto_v2` without a `landing_`/`staging_` prefix. This is the test that matters; everything else is bookkeeping.
   - `test_module_imports_without_pyodbc`
2. - [ ] Run and confirm failure.
3. - [ ] Implement `ops/truncate_v2_scratch_tables.py` with a guarded `pyodbc` import.
4. - [ ] Add `tests.test_v2_scratch_cleanup` to `UNIT_TEST_MODULES`; run the CI-venv repo checks.
5. - [ ] Wire it as the `staging_cleanup` stage in `new_portal_rto_monthly.sh` (Task 4), positioned **after** `post_ingest_check` so a failed verification still leaves the loaded staging rows available for diagnosis.
6. - [ ] On the VM, run `--dry-run` first and confirm it reports 160,573 rows in each table before running it for real.
7. - [ ] Commit.

**Rollback:** a `TRUNCATE` is not recoverable, so the safety argument has to hold before the fact, not after. It holds: both tables are rebuilt from scratch by the next ingest (each path truncates before loading), the only data they contain is a copy of a year that already exists in the fact table and in an immutable blob snapshot, and nothing reads them between runs. If a truncate is nonetheless regretted, re-run `python3 -m new_portal.rto_bulk_ingest --year 2013` to repopulate both from the 2013 blob snapshot — but note that this also re-replaces 2013 in the fact table, so prefer leaving them empty. Take no backup of these two tables; backing up scratch state would institutionalise the confusion this task exists to remove.

---

## Task 6: First supervised production run

**Files:** none. This task produces evidence, not code.

`docs/new_portal_v2_design.md` records an open prerequisite twice: *"Still needs one real full-scale run before cron, to confirm eight sessions stay healthy for the whole run."* That prerequisite is now **partly** met: the 2026-09-24 run completed a full 2026 sweep in 31 minutes, so eight sessions demonstrably survive a clean run. What is still unproven is a *degraded* run — the multi-hour, many-sweep case the `MAX`/`WAIT` budget exists for — and the new stages this plan adds (gate, post-ingest check, scratch cleanup) have never run in sequence against production. Scheduling that unattended is how you discover at 04:00 UTC that session eight dies at minute ninety.

### Steps

1. - [ ] Take a pre-run backup of the target table and record the row count:
   `SELECT * INTO dbo.fact_ev_data_by_rto_v2_backup_<YYYYMMDD> FROM dbo.fact_ev_data_by_rto_v2;`
2. - [ ] Run `bash new_portal_rto_monthly.sh` attended, on the VM, under `nohup setsid`. Record: wall clock, number of sweeps to completion, sessions created, peak gap count, whether the circuit breaker tripped.
3. - [ ] Confirm the gate passed for the right reasons, not vacuously — check the logged `expected_offices` against the live RTO count (currently 1,676 targets; `ops/new_portal_progress_count.py` prints progress against that number).
4. - [ ] Reconcile the newly ingested month against `get_dashboard_count()` at per-RTO grain for a sample, per `docs/new_portal_v2_design.md` §"ongoing validation" — flag anything over ~1–2% drift. A state-level rollup can pass while individual RTOs are wrong in cancelling directions.
5. - [ ] Write the measured numbers into `docs/runbooks/new_portal_rto.md` (Task 7) so the chosen `MAX`/`WAIT`/`STALL_LIMIT` have evidence behind them.
6. - [ ] Only then proceed to Task 7.

**Rollback:** `DELETE FROM dbo.fact_ev_data_by_rto_v2 WHERE [year] = <Y> AND [status_scope] = 'ALL_STATUSES';` then re-ingest from the immutable blob snapshot of the last known-good run (that is what the snapshots are for). Or, if the table was small enough to back up wholesale in step 1, restore from `fact_ev_data_by_rto_v2_backup_<YYYYMMDD>`. **Keep that backup table** until at least two further clean monthly runs have landed.

---

## Task 7: Install the schedule

**Files:** modify `ops/production_vm.crontab`, create `docs/runbooks/new_portal_rto.md`.

### Proposed cron lines (all UTC, appended after the three existing V1 entries)

```cron
30 03 3 * * bash /home/climate_dot_data/DataScraper_VahanParivahan/new_portal_rto_monthly.sh >> /home/climate_dot_data/DataScraper_VahanParivahan/new_portal_rto_monthly_logs.txt 2>&1
30 03 5 * * bash /home/climate_dot_data/DataScraper_VahanParivahan/new_portal_rto_monthly.sh >> /home/climate_dot_data/DataScraper_VahanParivahan/new_portal_rto_monthly_logs.txt 2>&1
15 06 * * * bash -lc 'cd /home/climate_dot_data/DataScraper_VahanParivahan && . venv/bin/activate && python3 ops/check_pipeline_freshness.py --table fact_ev_data_by_rto_v2 --status-scope ALL_STATUSES --max-load-age-days 35 --min-month-total-ratio 0.85 --pipeline new_portal_rto_freshness' >> /home/climate_dot_data/DataScraper_VahanParivahan/freshness_check_logs.txt 2>&1
```

### Rationale

**Why day 3, not day 1 — now measured.** The three V1 entries all fire on day 1: `30 18` state, `00 19` oem, `30 23` rto. Durations from the last real run (2026-10-01/02): **State 18:30 to 18:34 (~4 min), OEM 19:00 to 20:51 (~1h51m), RTO 23:30 to 00:39 next day (~1h10m).** So the whole V1 block clears by roughly **00:40 UTC on day 2** — considerably earlier than the "well into day 2" this plan's first draft assumed.

Day 3 at 03:30 UTC therefore rests on a measured **~27-hour margin** after V1 RTO finishes, not on an unmeasured guess. That margin is far wider than strictly necessary, and keeping it is a deliberate choice rather than an accident of ignorance: V1 RTO's 1h10m is one sample, Selenium runs are the fragile ones, and the cost of the margin is zero now that the day-3 window has to exist anyway for the freshness and retry logic. The independent reason day 3 still beats day 2 is the data, not the clock — the portal revises past months (measured: two RTOs moved +0.6% and +0.9% between consecutive daily runs), so two extra days of settling is worth having.

**Why 03:30 UTC (09:00 IST).** It sits nowhere near the 18:30-23:30 UTC V1 block on any day, so no future V1 schedule change collides with it. A complete sweep took **31 minutes** at the old unsafe pacing (2026-09-24, 17:20:01 to 17:51:17), but at the required `--sleep-seconds 0.45` the same work is **~2–2.5 hours**, and Task 4's `MAX`/`WAIT` budget allows a worst case near 5 hours. So the window closes by ~08:30 UTC typically and ~08:30–09:00 UTC at the outside — still inside an Indian working day if someone needs to act on an alert, which is the property that matters, but it is **no longer a comfortable margin**. If the portal tightens its bucket further, move the start earlier rather than cutting `MAX`.

**Why a second run on day 5.** This is the outage tolerance the constraints demand. The portal's bad spells last hours and `rto_fetch` checkpoints per office, so the day-5 run *resumes* rather than restarts — it costs little when day 3 already succeeded and recovers the month when day 3 hit an outage. It is safe to run twice because the gate is independent of which attempt produced the CSV, and because ingestion replaces the year wholesale: two clean runs produce the same state, just with a fresher snapshot. This also matches the design doc's stated intent to run "several times a month to capture the previous month repeatedly and track how it settles". **Note the coupling:** Task 2's `--max-load-age-days 35` is derived from this retry day. Move day 5 and that threshold must move too.

**Why the load-recency check is daily, not monthly.** It is the thing that notices nothing happened; running it monthly would discover a missed window a month late. Daily at 06:15 UTC, after the V2 window on run days and harmlessly on every other day. It keys on `MAX([inserted_at])` with a 35-day limit (Task 2), so it does **not** depend on guessing which month should be present — which is exactly the trap the first draft fell into. Against the real incident: the last load was 2026-09-24, so this check would have alerted from **2026-10-29** onward, every single day, instead of the two days of silence that actually happened.

**VM uptime window — the hard dependency.** The external automation that powers this VM currently needs to cover roughly 18:00 UTC day 1 through ~00:40 UTC day 2 (measured above). This schedule additionally requires:

- **03:00-07:00 UTC on day 3 and day 5** (V2 primary and retry, plus the post-run checks)
- **06:00-06:30 UTC every day** (daily load-recency check)

The daily requirement is the expensive one. If a daily power-on is not acceptable, the honest fallback is to drop the on-VM check to **day 7 only** (`15 06 7 * *`) and lean on the off-VM GitHub Actions watchdog (Task 3) for daily coverage — which is strictly better anyway, because it is the only check that works when the VM is off. **This is a maintainer decision, not an implementer one.** Do not install the crontab until the uptime window has been confirmed with whoever owns that automation; a cron line on a powered-off VM is exactly the failure this plan exists to fix.

### Steps

1. - [ ] **Re-check the `--min-month-total-ratio` band after the first two scheduled runs.** Calibration is done (see Task 2's table) and 0.85 is chosen, but the band rests on seven month-over-month samples from one year and the margin is only ~1.1 points wide. After two scheduled months have landed, recompute the healthy range including them, and widen or narrow 0.85 accordingly. Record the updated range in the runbook each time, so the number always has a date and a sample size attached to it.
2. - [ ] Confirm with the maintainer which VM uptime windows the external automation can actually provide, and pick the daily-vs-day-7 variant of the load-recency line accordingly. Record the answer in the runbook.
3. - [ ] On the VM, snapshot the live crontab before touching it: `crontab -l > ~/crontab_backup_$(date -u +%Y%m%d).txt`. It was verified byte-for-byte against `ops/production_vm.crontab` on 2026-11-03, so this is a rollback artifact rather than an investigation — but take it anyway, because the two are separate sources of truth with no automation syncing them.
4. - [ ] Add the three lines to `ops/production_vm.crontab`, with a comment block stating the required uptime windows, the measured V1 durations that justify day 3, and the `--max-load-age-days` / retry-day coupling. Commit.
5. - [ ] Install: `crontab /home/climate_dot_data/DataScraper_VahanParivahan/ops/production_vm.crontab`, then `crontab -l` and diff against the committed file.
6. - [ ] Write `docs/runbooks/new_portal_rto.md`: the schedule and why; what each alert shape means (`new_portal_rto` FAILED at `completeness_gate` = fetch incomplete, nothing ingested, safe to re-run; FAILED at `ingestion` = snapshot exists, re-run ingest only; `new_portal_rto_freshness` FAILED on **load recency** = no run has landed, VM was up, investigate the schedule; FAILED on **month-settled** = a run landed but captured an in-flight month, re-run now that the month is closed; GH Actions watchdog FAILED = **no run happened at all, check whether the VM started**); how to re-run one month by hand; the September-2026 stub as the worked example of why `MAX([date])` is not a health signal; the rollback steps from Tasks 5 and 6.
7. - [ ] Commit.

**Rollback (crontab):** `crontab ~/crontab_backup_<YYYYMMDD>.txt` restores the previous live crontab exactly, and `git revert` the commit to `ops/production_vm.crontab`. Both are needed — the file and the live crontab are separate sources of truth and this repo has no automation that syncs them.

**Rollback (database):** nothing in this task writes to the database. If a scheduled run has since ingested badly, use Task 6's rollback.

---

## Review Focus

The five things most likely to bite in production that **no test in this plan exercises**, most likely first:

1. **The VM is still off on day 3.** Every task here assumes the machine is powered; nothing in this repo can start it, and the GH Actions watchdog only tells you *afterwards* that it was not — up to 24 hours later, and only if the blob and webhook secrets are still valid.
2. **`--min-month-total-ratio 0.85` has only ~1.1 points of room.** The healthy minimum measured is 0.8878 and the stub is 0.7763, so the threshold sits ~3.8 points from each side on seven samples from a single year — a genuinely weak trading month trips it, and a mildly-less-partial stub slips under it. It is the only automated check on whether a month actually *settled*, and the band needs re-checking as real months land (Task 7 step 1).
3. **The sweep's wall clock exceeds its `MAX`x`WAIT` budget in a degraded month**, so the gate correctly refuses, the run alerts, and the month waits for the day-5 retry — which can also time out, producing two failure alerts and no data, with no third attempt scheduled before `--max-load-age-days 35` would even notice.
4. **Eight concurrent sessions degrade over a multi-hour retrying sweep** in ways the 31-minute clean run never showed (session expiry, portal-side per-IP throttling accumulating across sweeps). The circuit breaker catches a dead portal, not a slowly rotting session pool.
5. **`rto_bulk_ingest` falls back to `rto_ingest` on a multi-hour row-by-row path** that runs past the VM's power-off time and is killed mid-transfer, leaving the year deleted-but-not-reinserted — and `staging_cleanup` then never runs, so staging is left holding the rows that would have repaired it.

## Verified Facts (measured 2026-11-03, replacing this plan's earlier Open Questions)

- **Live crontab matches `ops/production_vm.crontab` byte-for-byte** — the three day-1 entries at `30 18`, `00 19`, `30 23` UTC, same paths, same `>>` redirects. CLAUDE.md's assertion is correct.
- **V1 durations:** State ~4 min, OEM ~1h51m, RTO ~1h10m (23:30 to 00:39 day 2). The V1 block clears by ~00:40 UTC on day 2.
- **V2 complete-sweep duration: 31 minutes** (2026-09-24 17:20:01 to 17:51:17), against the design doc's ~45 min extrapolation.
- **`fact_ev_data_by_rto_v2`:** 2,364,454 rows; 2026 covered for months 1-9 only; **no month-10 rows**; `MAX([date])` = September 2026; single `status_scope` value `ALL_STATUSES` throughout, confirming the ACTIVE drop in data.
- **September 2026 is a 3-day stub:** captured 2026-09-24 17:20-17:51, 15,049 rows / 239,579 EV vs V1's settled 332,523 = **-27.95%**. Months 1-8 drift only +0.14% to +1.70% against V1, so the pipeline is healthy and only the in-flight month is short.
- **`landing_fact_ev_data_by_rto_v2` and `staging_fact_ev_data_by_rto_v2` each hold 160,573 rows of 2013 data** (ym 201301..201312, 36 states) from an old backfill. Verified in code that both ingest paths truncate before loading, so this is a manual-recovery foot-gun rather than an ingestion bug — see Task 5.
- **`inserted_at DATETIME NOT NULL DEFAULT GETDATE()`** is present as the final column of the fact table, so Task 2's primary query is available today with no migration.
- **Row count cannot detect a stub month at any threshold; `SUM([total])` can.** Measured per-month ratios for 2026 / `ALL_STATUSES`:

  | mo | rows | row_ratio | total | tot_ratio | ev | ev_ratio |
  |---|---|---|---|---|---|---|
  | 1 | 15,854 | — | 2,845,484 | — | 229,080 | — |
  | 2 | 16,161 | 1.0194 | 2,526,268 | 0.8878 | 203,765 | 0.8895 |
  | 3 | 16,130 | 0.9981 | 2,819,910 | 1.1162 | 292,018 | 1.4331 |
  | 4 | 16,179 | 1.0030 | 2,725,660 | 0.9666 | 251,587 | 0.8615 |
  | 5 | 15,884 | 0.9818 | 2,577,306 | 0.9456 | 276,554 | 1.0992 |
  | 6 | 16,162 | 1.0175 | 2,603,196 | 1.0100 | 310,590 | 1.1231 |
  | 7 | 16,502 | 1.0210 | 2,632,295 | 1.0112 | 332,523 | 1.0706 |
  | 8 | 16,261 | 0.9854 | 2,452,785 | 0.9318 | 301,937 | 0.9080 |
  | **9** | **15,049** | **0.9255** | **1,904,174** | **0.7763** | **239,579** | **0.7935** |

  A partial-month capture keeps full breadth (36 states, 258 offices, every office reporting) and shrinks only the counts inside each row, so `COUNT(*)` — which measures breadth — barely moves: the stub's 0.9255 sits above any usable threshold. `SUM([total])` separates (healthy 0.8878–1.1162, stub 0.7763). EV sum separates too but false-alarms (healthy 0.8615–1.4331). See Task 2.
- **There is no `new_portal` storage key in the VM's `config.yaml`.** The complete `storage` block is `connection_string`, `container_name`, `csv_container_name`, `key`, `rto_wise_container_name`, `rto_wise_csv_container_name`, `state_wise_container_name`, `state_wise_csv_container_name`. So production runs on `rto_blob_snapshot.DEFAULT_CONTAINER_NAME = "new-portal-rto-csv"` via `resolve_container_name`'s fallback. Task 3's off-VM watchdog hard-codes that name — see the note there about the trap this sets.

## Still Open

- **Whether a GitHub-hosted runner can reach the Azure SQL server.** The firewall allow-list is not in this repo. Task 3 therefore watches *blob snapshot age*, which is reachable with a connection string. If SQL turns out to be reachable, an off-VM `MAX([inserted_at])` watchdog would be strictly stronger and should replace the blob proxy — note `inserted_at`, not `MAX([date])`, for the reason in Task 2.
- **Whether external automation can power the VM daily.** This decides the shape of Task 7's load-recency line and is the single biggest open item.
- **Why October 2026 never ran.** The table has no month-10 rows and no run landed after 2026-09-24. This plan prevents the *silence*, but the root cause of that specific miss (the unpaid-dues VM stop, per the incident description) is outside the repo and is not re-verified here.
- **429 backoff is unimplemented, and pacing is only a workaround.** `SLEEP_SECONDS=0.45` keeps us under the bucket by running ~35% slower than necessary and by assuming the bucket stays where it was measured on 2026-11-03. Neither holds forever. The actual fix is for `client.py` to treat 429 as its own case — honour `Retry-After` if present, otherwise sleep past the ~20s lockout and retry **without** counting the office as a gap — so a throttled call stops being indistinguishable from an empty office and stops feeding the circuit breaker. That is a change to `new_portal/client.py` and to `rto_fetch`'s gap accounting, it needs its own tests, and it is deliberately **out of scope here**: this plan schedules an existing pipeline and should not also redesign its retry policy. But until it exists, every scheduled run depends on a hand-tuned constant that the portal can invalidate without notice, and the failure mode is a silent multi-hour sweep that completes nothing. **Re-measure the bucket with ≥150s of sustained load before trusting `0.45` again** — a short burst always looks healthy, which is exactly how the original 0.2 default came to be believed safe.
- **Whether the pacing fix alone makes a *degraded* run survivable.** Task 7's attended run is still the prerequisite. Tonight's failure proved only that the unpaced default cannot finish; it did not exercise the gate, the post-ingest check or the new stages in sequence against production.
