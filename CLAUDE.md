# Claude Code Handover

This file is the LLM-oriented handover for this repository. It is written for an agent that needs to continue cleanup and improvement work without re-discovering the same production constraints.

## ⚠️ Read This First: Vahan Portal Retirement (as of 2026-08-09)

**The Vahan dashboard this entire pipeline depends on stops being usable for us after August 15, 2026.** The government is replacing it with a new portal that (a) has a different data grain than what this repo currently ingests and (b) does not support downloading raw data at all. Starting the month after cutover, `RTO`/`OEM`/`State` data will need to come from that new portal instead.

Consequences for anyone picking this repo up:

- **Development on this Vahan-based pipeline is intentionally halted as of 2026-08-09.** Do not invest in refactors, centralization work, or other code-quality improvements here unless something breaks and needs a fix, or the maintainer explicitly asks for it. The code's remaining useful life is measured in days, not the weeks/months this file's older "Recommended Future Work" section assumed.
- The **new-portal integration is a separate, not-yet-scoped initiative.** It has not started. Do not assume this repo's schema, grain, or ingestion pattern carries over — the new source is coarser and has no raw export, so the curated models will very likely need to change shape, not just source. See "Next Initiative: New Portal Integration" below.
- **This repo has been worked on by more than one AI agent in parallel** — Claude Code and an agent called "Codex" (see `codex/*` branches in git history, e.g. PRs #14-#17). If you're an agent picking this up, check `git log --oneline -15` before assuming any file is in the state you last saw it in — do not assume you're the only one who has touched this repo recently.
- The historical dataset (2013 through the last captured month) has been thoroughly remediated and verified as of 2026-08-09 — see "August 2026 Historical Remediation" below before re-investigating data-quality issues that may already be understood and fixed.

As of July 25, 2026, this project is mid-rewrite: the raw schema cleanup, shared preprocessing/ingestion/upload foundations, runtime alerting, and CI/repo checks are in place, but the Selenium scraping layer is only partially centralized. Most of that rewrite work is now deprioritized — see the retirement note above.

## What This Project Does

This repository collects monthly EV registration data from Vahan dashboard downloads at three grains:

- `RTO`
- `OEM`
- `State`

The production operating pattern is:

1. Scrape XLSX reports from the Vahan dashboard.
2. Run a second missing-file recovery scrape pass.
3. Preprocess raw XLSX files into a monthly CSV.
4. Load the CSV into SQL Server staging.
5. Delete the target month from the raw fact table.
6. Insert refreshed rows into the raw fact table.
7. Upload raw XLSX files and monthly CSV to Azure Blob Storage.
8. For `RTO` and `OEM`, run dbt curated models after raw ingestion finishes.

## Production Reality

Do not assume this repository is the full production truth.

- Production runs on an Azure VM.
- The VM is started and stopped by external automation that is not represented as IaC in this repo.
- The VM is **off by default**. The maintainer can turn it on and provide the exact SSH command on request — ask when you need to smoke-test scrapers end-to-end (this is the only real validation path for Selenium changes; Vahan blocks headless).
- The `main` branch was confirmed up to date and pulled on the VM as of 2026-07-25.
- Secrets live in `config.yaml` **on the VM** (gitignored, not in the repo); shared modules read them via `runtime_config.load_config()`. The repo-root `oauth-client.json` is gitignored and unreferenced by any code — a stray leftover, safe to remove.
- The repo-root `rto_state_office_mapping.json` (renamed 2026-07-29 from the former `output.json`) is **not dead** — do not delete it. It's the live state→RTO-office mapping cache for the RTO pipeline: `rto_level/rto_level_data_scraper.py` reads it as a fallback and overwrites it with the merged mapping every run (`load_previous_mapping()` / `main()`), and `rto_level_get_missing_files.py` and `telangana_historical_backfill.py` also read it (via the shared `RTO_STATE_OFFICE_MAPPING_PATH` constant). It is correctly gitignored (should never be committed), but the working-tree copy on the VM is load-bearing runtime state, not a stray file. The old `output.json` was incorrectly removed from git with a bare `git rm` on 2026-07-29 (should have been `git rm --cached`), which deleted the VM's live copy too; it was recovered from a VM-side `git stash`. If the mapping file is ever missing on the VM, the scraper still runs — it just loses the fallback for any state whose live mapping refresh fails that run (see `rto_level_data_scraper.py::load_previous_mapping`).
- The VM can drift from git history if files are edited directly on the machine.
- There is no real lower environment.
- Most changes go straight to production behavior once deployed.

This means every change should be made with rollbackability and production safety in mind.

## Current Safe Baseline

As of 2026-08-09, `main` is at `a28620e3` (`Merge pull request #17 from Climate-Dot/codex/historical-rto-reprocessing`). Recent commits of note, newest first:

- `a28620e3` merge PR #17 `codex/historical-rto-reprocessing` — `ab39a7a9` `Handle historical Vahan total columns`
- `a6d38a1c` merge PR #16 `codex/preserve-mixed-numeric-counts` — `311fc19f` `Preserve numeric objects during count normalization` (refines the fix below; see remediation section)
- `853b1d64` merge PR #15 `codex/rto-reingestion-repair` — `865fd039` `Replace complete snapshots safely during ingestion` (the orphaned-row-gap fix, see below)
- `27d6b707` merge PR #14 `fix/rto-office-selector-exact-match` — `0f1a7da5` `Fix RTO office selector matching the wrong office by code substring`

`a28620e3` is the last *functional* code change (data/pipeline behavior); commits after it (starting with `f79d9095`, this file's own documentation update) are docs-only. Given the portal retirement (see top of file), do not expect or require a long-lived "safe baseline" discipline here going forward — check `git log` for the actual current tip instead of trusting this section to stay current.

## August 2026 Historical Remediation (Complete)

Between 2026-08-04 and 2026-08-09, a user-reported data mix-up (one Haryana office's monthly report showing a different office's numbers) triggered a full audit and remediation of the entire historical RTO dataset (2013 through the latest captured month), done jointly by Claude Code and Codex. This work is now complete and verified; treat the dataset as trustworthy unless you find new, specific evidence otherwise. Bugs found and fixed, in the order discovered:

1. **RTO office-selector substring collision.** The Selenium office-picker XPath used `contains(text(), code)`, a substring match — a short code like `HR2` also matched longer codes starting with it (`HR29`, `HR269`) that existed in the same state, causing the wrong office's report to download under the wrong folder. Root-caused to a Feb 2025 commit; had been silently wrong for years for some offices. Fixed with an anchored `" - {code}("` match (`0f1a7da5`).
2. **Numeric fuel-count columns rendering as `"12.0"` instead of `"12"`.** When `pd.concat()` combines many offices' reports for a month and a fuel column (e.g. `pure_ev`) is present in some reports but absent in others, pandas NaN-fills the gaps and upcasts the whole column to `float64`, so real integer counts get written to CSV with a trailing `.0`, which then fails the curated dbt model's `CAST(... AS INT)`. First fix (`1cacf568`) used `if series.dtype == object` before stripping commas — this missed a further edge case (below). Refined fix (`311fc19f`) normalizes with `.astype("string")` unconditionally before the `.str.replace()` call.
3. **The refinement in (2) was needed because of a specific pandas footgun**: an `object`-dtype column can hold a genuine *mix* of real Python numbers and strings (e.g. some offices' `PURE EV` cell parsed by openpyxl as an `int`, others as text). Calling `.str.replace()` on such a column silently converts every non-string element (including real integers) to `NaN` — pandas' `.str` accessor requires actual strings and does not error on non-string input. This is why `pure_ev` was correct in isolated single-file tests (no mixed-dtype column ever formed) but wrong for a data-dependent subset of offices in full nationwide production runs — it depended on whether that specific office's raw cell happened to be typed as a number vs. text in its own report, not on how much data was processed together. Verified fixed: 100% non-null `pure_ev` across every year 2013-2026, spot-checked accurate against fresh raw file re-downloads (e.g. Gujarat/`GJ4` May 2026 = 148, Andhra Pradesh/`AP131` April 2026 = 263, both matching exactly).
4. **Orphaned-row gap in ingestion.** `build_delete_query()` originally only deleted final-table rows matching a staging row on the merge key — if an office's fresh report was legitimately empty that month, it contributed zero staging rows, so the old (possibly wrong) row for that office/month was never deleted. Fixed (`865fd039`) by adding an optional `replacement_scope_columns` param: the standard monthly `data_ingest()` now scopes the delete to `["date"]` alone (replaces the whole day's snapshot unconditionally), while Telangana's historical backfill scopes to `["date", "state"]` to avoid touching other states.
5. **Duplicate-data-fingerprint bug**: a small number of offices had a *correctly titled* report whose underlying vehicle-count data was actually a byte-for-byte copy of a different office's real numbers (a residual of the same concurrency race that caused bug #1, at a rate the original selector-mismatch validation couldn't detect since it only compared title-vs-folder, not cross-office data duplication). Found and fixed for the `HR16`/`HR241`/`HR254`/`HR268` cluster (Haryana, Jun 2026) and `TG2`/`TG28` (Telangana, Apr 2020) — both confirmed via live re-download from Vahan showing the "duplicate" office's true data was actually empty. A nationwide fingerprint sweep found no other statistically-significant (≥4 matching categories) clusters; ~600+ "weak" (1-3 category) matches remain and are very likely coincidental — verified via live re-download on a sample, not exhaustively.
6. **Office-rename duplication.** Vahan renamed roughly two dozen RTO offices while keeping the same `rto_code` — 23 in a portal-wide rename landing ~May 2026, plus `DL5` which has carried two names since 2018. Since this pipeline creates a folder (and therefore a DB row) per `(rto_code, current display name)` at scrape time, a renamed office gets **two** folder trees, and any month where both have data becomes a duplicate/double-counted row. **This is not code-fixed** — deliberately deferred given the portal retirement timeline (a code fix only matters for *future* full reprocessing runs, and there won't be many more of those). It has been manually deduped in the database twice (2026-08-05, 2026-08-09) after two different full historical reprocessing runs regenerated it from the still-dual-named source folders. **If anyone runs another full historical reprocess before this pipeline is retired, this will very likely reappear and need re-deduping.** See `fact_ev_data_by_rto_dedup_backup_20260805` and `fact_ev_data_by_rto_dedup_backup_20260809` for exactly what was removed each time, and the affected code list: `AP707, DL1, DL2, DL3, DL4, DL5, DL6, DL7, DL8, DL9, DL10, DL11, DL12, DL13, DL52, DL53, MH16, MH58, MN11, MP16, MZ9, PY51, TN631, UP321`. The dedup logic: for each code, pick the `rto_name` whose data extends to the latest month as canonical, then for any `(year, month)` where the canonical name *also* has a row, delete the non-canonical name's row for that same month only — this correctly preserves e.g. `DL5`'s legitimate 2013-through-mid-2018 history that only ever existed under its old name.

Also refreshed in this window: `rto_code_to_district_mapping` (SCD1, backed up to `rto_code_to_district_mapping_backup_20260805`, 1,661 rows reloaded from an updated CD mapping spreadsheet) — this specific migration's PR (`data/rto-district-mapping-refresh-20260805`) was never merged even though the data change is live in production; low-priority git-hygiene gap, not a data-correctness issue.

**Backup tables from this window — keep all of these.** With Vahan access closing 2026-08-15, none of these can be regenerated from source after that date, so treat them as irreplaceable until someone explicitly decides they're no longer needed: `fact_ev_data_by_rto_backup_20260808_pre_history_rebuild` (Codex's full-table snapshot immediately before the historical reprocess — the single most valuable one), `fact_ev_data_by_rto_dedup_backup_20260805`, `fact_ev_data_by_rto_dedup_backup_20260809`, `fact_ev_data_by_rto_hr16dup_backup_20260806`, `fact_ev_data_by_rto_tg2fix_backup_20260806`, `rto_code_to_district_mapping_backup_20260805`, plus the pre-existing 2026-06-19 schema-migration backups (`fact_ev_data_by_rto_backup_20260619`, `fact_ev_data_by_state_backup_20260619`, `fact_oem_data_by_state_and_category_backup_20260619`, `oem_wise_ev_data_backup_20260619`, `rto_wise_ev_data_backup_20260619`).

Final validation, verified end-to-end after all of the above: `pure_ev` 100% non-null and raw/curated row counts in sync (2,291,901 = 2,291,901) across the full dataset, zero remaining office-rename overlaps, and a full nationwide title-vs-folder blob sweep re-run one more time as a last-chance check before the 2026-08-15 cutoff.

## What Is Already Done

### Raw schema and taxonomy cleanup

- The raw fuel taxonomy has been standardized across `RTO`, `OEM`, and `State`.
- Active preprocessing creates missing expected columns as `NULL`, not `0`.
- Shared fuel naming is aligned across the three pipelines.
- `inserted_at` is the standardized metadata column.
- SQL migration assets exist to recreate raw and staging tables safely with backups.

Important intent:

- New Vahan columns should be preserved as first-class columns where possible.
- New columns should not be forced back into legacy aliases unless the business explicitly wants that behavior.
- Unmappable legacy values should remain `NULL`, not fabricated.

### Shared Python foundations

These are already centralized and should be reused instead of re-implemented:

- [`pipeline_constants.py`](/Users/monish/DataScraper_VahanParivahan/pipeline_constants.py)
- [`runtime_config.py`](/Users/monish/DataScraper_VahanParivahan/runtime_config.py)
- [`etl_preprocessing.py`](/Users/monish/DataScraper_VahanParivahan/etl_preprocessing.py)
- [`etl_ingestion.py`](/Users/monish/DataScraper_VahanParivahan/etl_ingestion.py)
- [`etl_blob_upload.py`](/Users/monish/DataScraper_VahanParivahan/etl_blob_upload.py)
- [`preprocessing_schema_utils.py`](/Users/monish/DataScraper_VahanParivahan/preprocessing_schema_utils.py)
- [`sqlserver_utils.py`](/Users/monish/DataScraper_VahanParivahan/sqlserver_utils.py)
- [`blob_storage_utils.py`](/Users/monish/DataScraper_VahanParivahan/blob_storage_utils.py)
- [`pipeline_logging.py`](/Users/monish/DataScraper_VahanParivahan/pipeline_logging.py)
- [`utils.py`](/Users/monish/DataScraper_VahanParivahan/utils.py) — Selenium **primitives** and misc helpers (`find_element`, `open_page`, `detect_blocked_page`, `capture_browser_diagnostics`, `wait_for_expected_download`, Chrome option builders, date/Excel helpers). This is a committed shared foundation.

Selenium design (intended end state, kept deliberately simple as two layers):

- `utils.py` (committed) holds generic, page-agnostic Selenium primitives and helpers.
- `vahan_dashboard.py` (the page object attempted for this — see "Current Unfinished Work") would have held Vahan-specific `Locator`s and page actions (`select_state`, `select_year`, `download_current_report`, …) built on top of `utils.py`. It was never committed/promoted and is now abandoned given the portal retirement, not merely paused.
- The one-file-change-instead-of-three-file-hunt goal this was chasing is now moot for Vahan; keep the idea in mind for whatever the new-portal scraper looks like instead.
- Note: a stale `__pycache__/selenium_runtime.cpython-313.pyc` may exist with no matching source — that module was merged into `utils.py`; ignore/clean the pyc.

Thin-wrapper pattern (do not mistake for duplication):

- Per-pipeline scripts such as `*/upload_files_to_blob_storage.py`, `*/data_ingestion.py`, and `oem_level/data_preprocessing_v2.py` are **thin entrypoints that delegate** to the shared `etl_*` modules. They are the real production path invoked by the shell entrypoints; keep them.

### Runtime hardening

- Monthly shell entrypoints now use a shared runtime helper at [`ops/etl_runtime.sh`](/Users/monish/DataScraper_VahanParivahan/ops/etl_runtime.sh).
- Failure alerts and success alerts can be sent to Google Chat via [`ops/send_chat_alert.py`](/Users/monish/DataScraper_VahanParivahan/ops/send_chat_alert.py).
- `RTO` and `OEM` automatically run dbt after ingestion.
- The cron snapshot is documented in [`ops/production_vm.crontab`](/Users/monish/DataScraper_VahanParivahan/ops/production_vm.crontab).

### Browser runtime policy

This is critical.

- Vahan frequently blocks headless Selenium in production-like runs.
- The shell entrypoints default to `VAHAN_HEADLESS=false`.
- Selenium stages are wrapped through `run_selenium_step` in [`ops/etl_runtime.sh`](/Users/monish/DataScraper_VahanParivahan/ops/etl_runtime.sh).
- When headless mode is disabled, Selenium runs under `xvfb-run -a`.

Interpretation rule:

- `Access Forbidden` during initial page load is first a runtime/access problem, not automatically a selector problem.

### Validation and repo hygiene

- Shared repo validation exists at [`ops/run_repo_checks.sh`](/Users/monish/DataScraper_VahanParivahan/ops/run_repo_checks.sh).
- GitHub Actions CI exists in [`.github/workflows/ci.yml`](/Users/monish/DataScraper_VahanParivahan/.github/workflows/ci.yml).
- The dbt starter example models were removed.
- `config.example.yaml` exists as a placeholder contract for runtime configuration.

CI import-safety rule (critical, verified 2026-07-25):

- CI and `run_repo_checks.sh` install only `requirements-ci.txt` (PyYAML, openpyxl, pandas). The VM has the full `requirements.txt` (azure, pyodbc, …).
- Therefore any module imported by a unit test **must import without `azure` or `pyodbc` installed**. Use the guarded-import pattern already in `blob_storage_utils.py` (`try: import X / except ImportError: X = None`), as now done in `etl_blob_upload.py` and `etl_ingestion.py`.
- Reproduce CI locally: create a Python 3.12/3.13 venv with only `requirements-ci.txt`, then `PYTHON_BIN=<venv>/bin/python bash ops/run_repo_checks.sh`.
- History note: CI was silently red before 2026-07-25 because `etl_blob_upload.py`/`etl_ingestion.py` hard-imported `azure`/`pyodbc`. Fixed with guarded imports; `tests.test_vahan_dashboard` and `tests.test_pipeline_runtime_wiring` were also added to the runner's module list.

## Current Pipeline Shape

### RTO

- Shell entrypoint: [`rto_ev_data_etl.sh`](/Users/monish/DataScraper_VahanParivahan/rto_ev_data_etl.sh)
- Scraper: [`rto_level/rto_level_data_scraper.py`](/Users/monish/DataScraper_VahanParivahan/rto_level/rto_level_data_scraper.py)
- Missing-file pass: [`rto_level/rto_level_get_missing_files.py`](/Users/monish/DataScraper_VahanParivahan/rto_level/rto_level_get_missing_files.py)
- Preprocessing: [`rto_level/rto_level_data_pre_processing.py`](/Users/monish/DataScraper_VahanParivahan/rto_level/rto_level_data_pre_processing.py)
- Ingestion: [`rto_level/rto_level_data_ingestion.py`](/Users/monish/DataScraper_VahanParivahan/rto_level/rto_level_data_ingestion.py)
- Raw table: `fact_ev_data_by_rto`
- Curated model: `rto_wise_ev_data`

### OEM

- Shell entrypoint: [`oem_data_etl.sh`](/Users/monish/DataScraper_VahanParivahan/oem_data_etl.sh)
- Scraper: [`oem_level/oem_level_data_scraper.py`](/Users/monish/DataScraper_VahanParivahan/oem_level/oem_level_data_scraper.py)
- Missing-file pass: [`oem_level/get_missing_files.py`](/Users/monish/DataScraper_VahanParivahan/oem_level/get_missing_files.py)
- Preprocessing: [`oem_level/data_preprocessing_v2.py`](/Users/monish/DataScraper_VahanParivahan/oem_level/data_preprocessing_v2.py)
- Ingestion: [`oem_level/data_ingestion.py`](/Users/monish/DataScraper_VahanParivahan/oem_level/data_ingestion.py)
- Raw table: `fact_oem_data_by_state_and_category`
- Curated model: `oem_wise_ev_data`

### State

- Shell entrypoint: [`state_ev_data_etl.sh`](/Users/monish/DataScraper_VahanParivahan/state_ev_data_etl.sh)
- Scraper: [`state_level/state_level_data_scraper.py`](/Users/monish/DataScraper_VahanParivahan/state_level/state_level_data_scraper.py)
- Missing-file pass: [`state_level/state_level_get_missing_files.py`](/Users/monish/DataScraper_VahanParivahan/state_level/state_level_get_missing_files.py)
- Preprocessing: [`state_level/state_level_data_pre_processing.py`](/Users/monish/DataScraper_VahanParivahan/state_level/state_level_data_pre_processing.py)
- Ingestion: [`state_level/state_level_data_ingestion.py`](/Users/monish/DataScraper_VahanParivahan/state_level/state_level_data_ingestion.py)
- Raw table: `fact_ev_data_by_state`
- Curated model in repo: `state_wise_ev_data`

Important (deliberate, confirmed 2026-07-25):

- `state_wise_ev_data` exists in dbt, but `State` does **not** auto-run dbt from its shell entrypoint. This is intentional — nobody consumes the curated State data right now. Do not wire it into the production path without being asked.

## Historical / One-Off Path

Telangana historical RTO backfill exists and is intentionally isolated:

- Wrapper: [`rto_telangana_backfill.sh`](/Users/monish/DataScraper_VahanParivahan/rto_telangana_backfill.sh)
- Script: [`rto_level/telangana_historical_backfill.py`](/Users/monish/DataScraper_VahanParivahan/rto_level/telangana_historical_backfill.py)
- Workspace root: [`rto_level/historical_backfill/telangana`](/Users/monish/DataScraper_VahanParivahan/rto_level/historical_backfill/telangana)

Design intent:

- One-off historical recovery code should not pollute the monthly production paths.
- Backfill logic should be isolated in its own folder, inputs, logs, and temporary workspace.

## Current Unfinished Work

**Status update 2026-08-09: this refactor is abandoned, not paused.** Given the Vahan portal retirement (see top of file), it is very unlikely to be worth finishing — the scrapers it would refactor have only days of useful life left. It was never merged into `main`; the uncommitted bucket is preserved in a local git stash (`git stash list` on the machine that was doing this session's work) rather than deleted, in case it's ever wanted for reference on the new-portal scraper. Do not revive it without checking with the maintainer first, given the timeline.

As of July 25, 2026 (original note, kept for context), the only visible uncommitted refactor bucket was the Selenium selector/page-action centralization:

- modified: [`oem_level/oem_level_data_scraper.py`](/Users/monish/DataScraper_VahanParivahan/oem_level/oem_level_data_scraper.py)
- modified: [`rto_level/rto_level_data_scraper.py`](/Users/monish/DataScraper_VahanParivahan/rto_level/rto_level_data_scraper.py)
- modified: [`state_level/state_level_data_scraper.py`](/Users/monish/DataScraper_VahanParivahan/state_level/state_level_data_scraper.py)
- untracked: `vahan_dashboard.py`
- untracked: `tests/test_vahan_dashboard.py`

What it was trying to do:

- move shared selectors and Selenium page actions into one place
- reduce three-way duplication across `RTO`, `OEM`, and `State`
- make DOM drift fixes a one-file change instead of a three-file hunt

Why it was still risky (also now moot given the retirement):

- scraper entrypoints have slightly different control flows
- the Vahan UI is flaky and changes behavior between headless and headed sessions
- scraping breakage is operationally expensive because prod is the only real environment
- the refactor was never validated end-to-end on the VM

## How We Want This Project To Look

The desired end state is a boring, predictable, low-drama production data repo.

### Desired technical shape

- one shared Selenium interaction layer
- one shared missing-file recovery framework
- one shared preprocessing framework
- one shared ingestion framework
- one shared blob upload framework
- one consistent logging format across all jobs
- one clear alerting path for success and failure
- one documented cron/runtime source of truth
- one dbt project with explicit production conventions
- one isolated place for one-off backfills

### Desired operational shape

- no silent failures
- no mystery files living only on the VM
- no duplicated selectors across three pipelines
- no hidden schema assumptions
- easy rerun steps
- every risky migration accompanied by backup and rollback notes

### Desired repository shape

- keep active production code separate from legacy or historical reference scripts
- continue standardizing on lowercase snake_case module and directory names
- keep one-off scripts under explicit subfolders instead of the main monthly execution path
- prefer shared helpers over copy-pasted pipeline-specific implementations

## Next Initiative: New Portal Integration

This is the actual next major body of work, not the sections below. As of 2026-08-09 it has not been scoped — the maintainer wants to have that conversation directly rather than have an agent guess at an architecture. Known constraints going in, from the maintainer directly:

- The new portal has a materially different data grain than Vahan (exact difference not yet confirmed — could mean coarser geography, coarser vehicle/fuel categorization, coarser time granularity, or some combination).
- The new portal does **not** support downloading raw data, unlike Vahan's XLSX exports. Whatever capture mechanism is used will need to work from whatever *is* exposed (a rendered dashboard, an API, etc. — not yet confirmed).
- Do not assume the current three-grain (`RTO`/`OEM`/`State`) schema, the current curated dbt model shapes, or the current monthly delete-then-insert ingestion pattern all carry over unchanged. This repo's own stated principle — never fabricate data that isn't genuinely available, prefer `NULL` over an invented value — applies with extra force here: a coarser source must not be forced to look like the old fine-grained one.
- Start by understanding exactly what the new portal exposes (get a URL/example from the maintainer, inspect it directly) before proposing any pipeline design.

## Recommended Future Work (Deprioritized — Vahan Pipeline Only)

The sections below were the suggested order of work for continuing to improve the Vahan-based pipeline. Given the portal retirement, treat these as reference/context, not an active backlog — do not pick these up without the maintainer explicitly asking, and prefer routing effort toward the new-portal initiative above instead.

### 1. Finish the Selenium centralization safely

Goal:

- centralize selectors and shared page actions in `vahan_dashboard.py`

How:

- preserve pipeline-specific orchestration where necessary
- centralize only the repeated page interactions first
- keep tests for selector order and diagnostics
- smoke test each pipeline on the VM in non-headless `xvfb` mode before rollout

Acceptance bar:

- one successful end-to-end run each for `State`, `RTO`, and `OEM`
- log diagnostics still point to the failed step and captured artifacts if the page changes again

### 2. Centralize missing-file recovery

Current gap:

- scrape retry / missing-file recovery logic is still duplicated across pipelines

Target:

- one shared framework that can take pipeline-specific path builders and report iterators

### 3. Make dbt operating rules explicit

Current gaps:

- dbt schema targeting still depends on VM-local profile behavior
- `State` model exists but is not part of the automatic prod path

Target decisions:

- decide whether curated models should live in `dbo` or a dedicated curated schema
- decide whether `state_wise_ev_data` should become part of the production shell path
- add more dbt tests around nullability, uniqueness, and row-window assumptions

### 4. Reduce VM drift

Target:

- document all required runtime dependencies in one place
- make VM bootstrap reproducible
- eventually add IaC for VM lifecycle, cron bootstrap, and system packages such as `xvfb`

### 5. Strengthen operational observability

Current state:

- Google Chat alerts exist
- logs are better than before

Still worth doing:

- standardize log fields across all scripts
- add clearer summaries for expected file counts, downloaded counts, empty reports, and bad zip files
- add a simple post-run success/failure summary artifact if helpful

### 6. Continue cleanup of legacy scripts

Some scripts are reference-only and should not be treated as current production truth.

Before deleting anything:

- verify it is not used from cron, shell entrypoints, or manual backfill flow
- verify it is not the only surviving implementation of a business rule

## Safety Rules For Any Future Agent

1. Treat the VM as drift-prone until proven otherwise.
2. Prefer additive, reversible changes over sweeping rewrites.
3. Never rename or remap business columns without checking the actual raw file schema first.
4. If a raw Vahan column disappears, default missing output columns to `NULL`, not `0`, unless the business explicitly says otherwise.
5. When dbt reports duplicate model names, check for stale directories on the VM before assuming the repo is wrong.
6. Treat `Access Forbidden` as a browser/runtime problem before editing selectors.
7. Do not mix historical backfill logic into the monthly production path.
8. Do not assume `State`, `OEM`, and `RTO` have identical scraper flows even when they look similar.
9. If changing raw SQL tables, keep backups and explicit migration notes.
10. If something is only tested locally, do not call it production-safe.

## What To Read First

Start here, in order:

1. [`README.md`](/Users/monish/DataScraper_VahanParivahan/README.md)
2. [`docs/architecture.md`](/Users/monish/DataScraper_VahanParivahan/docs/architecture.md)
3. relevant runbook under [`docs/runbooks`](/Users/monish/DataScraper_VahanParivahan/docs/runbooks)
4. relevant shell entrypoint
5. relevant preprocessing and ingestion script
6. relevant dbt model under [`climate_dot_dbt/models/curated`](/Users/monish/DataScraper_VahanParivahan/climate_dot_dbt/models/curated)
7. the abandoned Selenium centralization work is reference-only now (see "Current Unfinished Work") — skip it unless specifically relevant

## Useful Validation Commands

Fast repo checks:

```bash
bash ops/run_repo_checks.sh
```

dbt parse when a profile is available:

```bash
RUN_DBT_PARSE=1 DBT_PROFILES_DIR=/path/to/dbt/profiles bash ops/run_repo_checks.sh
```

Quick view of unfinished work:

```bash
git status --short
```

## Final Guidance

If you are an agent picking this project up after 2026-08-09:

- Read the portal-retirement notice at the top of this file first. The Vahan pipeline is winding down, not being actively improved.
- The historical dataset has been thoroughly remediated — read "August 2026 Historical Remediation" before re-investigating data-quality issues, but don't assume it's infallible either; verify specific claims against the actual data before trusting them, the same way this remediation effort itself repeatedly found handover claims that needed correcting.
- Check `git log` for what's actually on `main` before assuming any file's state — more than one agent has been working on this repo.
- If the maintainer wants to talk about the new portal, that's the real next project — don't default to more Vahan-pipeline cleanup instead.
- centralize code where it reduces real maintenance burden, preserve improvements that made the project less fragile, and do not confuse "clean abstraction" with "production-ready change" — these older principles still apply to whatever code does get touched, they just apply to a much smaller remaining surface area now.