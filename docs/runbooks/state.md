# State Runbook

## Purpose

The State pipeline collects monthly EV registration data at the state grain, loads it into SQL Server raw tables, and uploads artifacts to Azure Blob Storage. A `state_wise_ev_data` dbt model exists in the repo for manual or future use, but it is not part of the current prod operating path.

## Entry Point

- Shell script: [`state_ev_data_etl.sh`](/Users/monish/DataScraper_VahanParivahan/state_ev_data_etl.sh)

## Current Flow

1. `State-level/state_level_data_scraper.py`
2. `State-level/state_level_get_missing_files.py`
3. `State-level/state_level_data_pre_processing.py`
4. `State-level/state_level_data_ingestion.py`
5. `State-level/upload_files_to_blob_storage.py`

Current note:

- The shell script does not yet run dbt automatically.
- The curated dbt model exists in the repo, but it is not currently materialized as part of the live prod workflow.
- The active state and OEM scrapers now share one centralized state list, including `Telangana`.

## Default Execution Behavior

- If no arguments are provided, the script targets the previous calendar month.
- If arguments are provided, they should be passed as `MON YEAR`, for example `OCT 2024`.

## Important Files

- Preprocessing: [`State-level/state_level_data_pre_processing.py`](/Users/monish/DataScraper_VahanParivahan/State-level/state_level_data_pre_processing.py)
- Ingestion: [`State-level/state_level_data_ingestion.py`](/Users/monish/DataScraper_VahanParivahan/State-level/state_level_data_ingestion.py)
- Blob upload: [`State-level/upload_files_to_blob_storage.py`](/Users/monish/DataScraper_VahanParivahan/State-level/upload_files_to_blob_storage.py)
- Curated model in repo: [`climate_dot_dbt/models/curated/state_wise_ev_data.sql`](/Users/monish/DataScraper_VahanParivahan/climate_dot_dbt/models/curated/state_wise_ev_data.sql)
- Shared constants: [`pipeline_constants.py`](/Users/monish/DataScraper_VahanParivahan/pipeline_constants.py)
- Shared schema helpers: [`preprocessing_schema_utils.py`](/Users/monish/DataScraper_VahanParivahan/preprocessing_schema_utils.py)

## Files and Tables Touched

- Downloaded XLSX files: `State-level/state_level_ev_data/<state>/<year>/<month>/reportTable.xlsx`
- Processed CSV: `state_level_ev_data_<MON>_<YEAR>.csv`
- Staging table: `staging_fact_ev_data_by_state`
- Raw final table: `fact_ev_data_by_state`
- Curated model target if manually materialized: `state_wise_ev_data`

## Manual Commands

End-to-end script:

```bash
./state_ev_data_etl.sh
```

Specific month:

```bash
./state_ev_data_etl.sh OCT 2024
```

dbt only, if you intentionally maintain this table:

```bash
cd /home/climate_dot_data/DataScraper_VahanParivahan/climate_dot_dbt
dbt parse
dbt compile --select state_wise_ev_data
dbt run --select state_wise_ev_data
```

## Operational Notes

- The preprocessing step uses the shared mapping workbook to derive vehicle dimensions.
- The preprocessing step uses the shared fuel taxonomy and logs unexpected raw columns.
- Missing expected output columns are written as `NULL`, not `0`.
- The current `state_wise_ev_data` dbt model reads from the state raw table directly.
- The model should only be full-refreshed if you intentionally choose to maintain it in your environment.
- The upload step removes local XLSX directories after blob upload and deletes the processed CSV after uploading it.

## Common Failure Points

- `config.yaml` missing or stale on the VM
- Missing downloaded XLSX files for a target month
- Mapping workbook drift
- Raw SQL tables on the VM not yet migrated to the newer shared schema

## Selenium Failure Triage

- Every Selenium lookup now logs a semantic `failed_step`, not just a raw stacktrace.
- Retry logs now include the full run context, for example state, month, and year.
- On each failed lookup, the scraper writes three debug artifacts under `debug_artifacts/selenium/state/`:
  - a screenshot `.png`
  - the page source `.html`
  - a metadata `.json` file with step name, locator, URL, and run context
- If a run fails, search the log for `failed_step=` first. That tells you whether the breakage was at initial page load, state selection, axis selection, year selection, refresh, or download.
- If the page title in the diagnostics JSON is `Access Forbidden`, treat it as a site-side block on the automated browser session, not selector drift.
- The metadata JSON path is included directly in the error log line, so you can open the saved HTML and screenshot without reproducing the issue live.

Example VM commands:

```bash
grep -n "failed_step=" /home/climate_dot_data/DataScraper_VahanParivahan/state_ev_etl_logs.txt | tail -20
find /home/climate_dot_data/DataScraper_VahanParivahan/debug_artifacts/selenium/state -type f | tail -20
```

## Verification Checklist

- Confirm the processed CSV exists before ingestion.
- Confirm raw table load completes without SQL errors.
- Compile the dbt model before intentionally materializing it in an environment.
- Confirm blob upload completed before local cleanup removed files.
- If you ran the raw schema migration, confirm the migration row counts match before and after backfill.
