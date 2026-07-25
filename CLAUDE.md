# Claude Code Handover

This file is the LLM-oriented handover for this repository. It is written for an agent that needs to continue cleanup and improvement work without re-discovering the same production constraints.

As of July 25, 2026, this project is mid-rewrite: the raw schema cleanup, shared preprocessing/ingestion/upload foundations, runtime alerting, and CI/repo checks are in place, but the Selenium scraping layer is only partially centralized.

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
- Secrets live in `config.yaml` **on the VM** (gitignored, not in the repo); shared modules read them via `runtime_config.load_config()`. The repo-root `oauth-client.json` and `output.json` are gitignored and unreferenced by any code (stray leftovers, safe to remove).
- The VM can drift from git history if files are edited directly on the machine.
- There is no real lower environment.
- Most changes go straight to production behavior once deployed.

This means every change should be made with rollbackability and production safety in mind.

## Current Safe Baseline

The latest known-safe committed baseline includes these recent commits:

- `d8bb5d16` `Add Telangana historical RTO backfill`
- `1e561f3d` `Fix Telangana backfill mapping resolution`
- `0256d78b` `Harden Telangana backfill runtime config`
- `a5b0a8e2` `Centralize ETL preprocessing, load, and upload flows`
- `0dcb106e` `Add rollout-safe repo checks and cleanup`

Treat `0dcb106e` as the current safe handoff point unless newer work is explicitly reviewed.

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
- `vahan_dashboard.py` (the in-progress page object — see "Current Unfinished Work") will hold Vahan-specific `Locator`s and page actions (`select_state`, `select_year`, `download_current_report`, …) built on top of `utils.py`. It is **not committed/promoted yet**; it and the three scraper edits that import it stay uncommitted until VM smoke tests pass.
- A DOM-drift fix should then be a one-file change in `vahan_dashboard.py`, not a three-file hunt across scrapers.
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

As of July 25, 2026, the only visible uncommitted refactor bucket is the Selenium selector/page-action centralization:

- modified: [`oem_level/oem_level_data_scraper.py`](/Users/monish/DataScraper_VahanParivahan/oem_level/oem_level_data_scraper.py)
- modified: [`rto_level/rto_level_data_scraper.py`](/Users/monish/DataScraper_VahanParivahan/rto_level/rto_level_data_scraper.py)
- modified: [`state_level/state_level_data_scraper.py`](/Users/monish/DataScraper_VahanParivahan/state_level/state_level_data_scraper.py)
- untracked: [`vahan_dashboard.py`](/Users/monish/DataScraper_VahanParivahan/vahan_dashboard.py)
- untracked: [`tests/test_vahan_dashboard.py`](/Users/monish/DataScraper_VahanParivahan/tests/test_vahan_dashboard.py)

This refactor is directionally good, but it is not yet the production-safe baseline.

What it is trying to do:

- move shared selectors and Selenium page actions into one place
- reduce three-way duplication across `RTO`, `OEM`, and `State`
- make DOM drift fixes a one-file change instead of a three-file hunt

Why it is still risky:

- scraper entrypoints have slightly different control flows
- the Vahan UI is flaky and changes behavior between headless and headed sessions
- scraping breakage is operationally expensive because prod is the only real environment
- the refactor has not yet been validated enough end-to-end on the VM

Rule:

- Do not merge the `vahan_dashboard.py` refactor into production just because the abstraction looks cleaner.
- It should be promoted only after targeted smoke tests for `OEM`, `RTO`, and `State` on the VM.

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

## Recommended Future Work

This is the suggested order of work for the next agent.

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
7. only then the unfinished Selenium centralization diff

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

If you are Claude Code picking this project up:

- centralize code where it reduces real maintenance burden
- preserve the recent improvements that made the project less fragile
- do not confuse "clean abstraction" with "production-ready change"