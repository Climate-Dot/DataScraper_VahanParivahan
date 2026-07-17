# OEM Runbook

## Purpose

The OEM pipeline collects monthly EV registration data at the OEM-by-state-and-category grain, loads it into SQL Server raw tables, uploads artifacts to Azure Blob Storage, and supports a curated `oem_wise_ev_data` dbt model.

## Entry Point

- Shell script: [`oem_data_etl.sh`](/Users/monish/DataScraper_VahanParivahan/oem_data_etl.sh)

## Current Flow

1. `oem_level/oem_level_data_scraper.py`
2. `oem_level/get_missing_files.py`
3. `oem_level/data_preprocessing_v2.py`
4. `oem_level/data_ingestion.py`
5. `oem_level/upload_files_to_blob_storage.py`
6. `dbt run --select oem_wise_ev_data`

Current note:

- The shell script now runs `dbt run --select oem_wise_ev_data` automatically at the end.
- The dbt output is written to `dbt_oem_wise_logs.txt` on the VM.
- The active state and OEM scrapers now share one centralized state list, including `Telangana`.

## Browser Runtime

- The shell script defaults to `VAHAN_HEADLESS=false`.
- Selenium steps in [`oem_data_etl.sh`](/Users/monish/DataScraper_VahanParivahan/oem_data_etl.sh) are routed through [`ops/etl_runtime.sh`](/Users/monish/DataScraper_VahanParivahan/ops/etl_runtime.sh).
- When headless mode is disabled, the Selenium steps run under `xvfb-run -a`.
- `xvfb-run` must be installed on the production VM.
- Only the Selenium steps use the `xvfb` wrapper. Preprocessing, ingestion, blob upload, and dbt run normally.

## Default Execution Behavior

- If no arguments are provided, the script targets the previous calendar month.
- If arguments are provided, they should be passed as `MON YEAR`, for example `OCT 2024`.

## Important Files

- Preprocessing: [`oem_level/data_preprocessing_v2.py`](/Users/monish/DataScraper_VahanParivahan/oem_level/data_preprocessing_v2.py)
- Ingestion: [`oem_level/data_ingestion.py`](/Users/monish/DataScraper_VahanParivahan/oem_level/data_ingestion.py)
- Blob upload: [`oem_level/upload_files_to_blob_storage.py`](/Users/monish/DataScraper_VahanParivahan/oem_level/upload_files_to_blob_storage.py)
- Curated model: [`climate_dot_dbt/models/curated/oem_wise_ev_data.sql`](/Users/monish/DataScraper_VahanParivahan/climate_dot_dbt/models/curated/oem_wise_ev_data.sql)
- Shared constants: [`pipeline_constants.py`](/Users/monish/DataScraper_VahanParivahan/pipeline_constants.py)
- Shared schema helpers: [`preprocessing_schema_utils.py`](/Users/monish/DataScraper_VahanParivahan/preprocessing_schema_utils.py)

## Files and Tables Touched

- Downloaded XLSX files: `oem_level/oem_data_by_state_and_category/<state>/<vehicle_class>/<year>/<month>/reportTable.xlsx`
- Processed CSV: `oem_data_by_state_and_category_<MON>_<YEAR>.csv`
- Staging table: `staging_fact_oem_data_by_state_and_category`
- Raw final table: `fact_oem_data_by_state_and_category`
- Curated model target: `oem_wise_ev_data`

## Manual Commands

End-to-end script:

```bash
./oem_data_etl.sh
```

Specific month:

```bash
./oem_data_etl.sh OCT 2024
```

Check VM browser prerequisites:

```bash
command -v xvfb-run
echo "${VAHAN_HEADLESS:-false}"
```

Google Chat alert smoke test:

```bash
python3 /home/climate_dot_data/DataScraper_VahanParivahan/ops/send_chat_alert.py \
  --pipeline oem \
  --status TEST \
  --run-label "manual smoke test" \
  --step setup
```

dbt only:

```bash
cd /home/climate_dot_data/DataScraper_VahanParivahan/climate_dot_dbt
dbt parse
dbt compile --select oem_wise_ev_data
dbt run --select oem_wise_ev_data
```

## Operational Notes

- The preprocessing step maps raw `maker` values into the curated `oem_name` field.
- The preprocessing step uses the shared fuel taxonomy and logs unexpected raw columns.
- Missing expected output columns are written as `NULL`, not `0`.
- The curated model reads from the newer raw fuel taxonomy directly.
- The upload step removes local XLSX directories after blob upload and deletes the processed CSV after uploading it.
- The shell entrypoint now stops on the first failed step and will not continue into dbt after an upstream failure.
- If a Google Chat webhook is configured, the shell entrypoint sends one alert on failure and one celebratory alert on successful completion.
- Failure alerts include the failed step, host, cron log path, and dbt excerpts when the dbt step fails.

## Common Failure Points

- `config.yaml` missing or stale on the VM
- Google Chat webhook missing or stale in `config.yaml` if alerting is expected
- dbt model path drift on the VM after folder renames
- Raw SQL tables on the VM not yet migrated to the newer shared schema
- Mapping file drift in [`Table and Mapping V2.xlsx`](/Users/monish/DataScraper_VahanParivahan/Table%20and%20Mapping%20V2.xlsx)
- `xvfb-run` missing on the VM while `VAHAN_HEADLESS=false`

## Selenium Failure Triage

- Every Selenium lookup now logs a semantic `failed_step`, not just a raw stacktrace.
- Retry logs now include the full run context, for example state, vehicle category, month, and year.
- On each failed lookup, the scraper writes three debug artifacts under `debug_artifacts/selenium/oem/`:
  - a screenshot `.png`
  - the page source `.html`
  - a metadata `.json` file with step name, locator, URL, and run context
- If a run fails, search the log for `failed_step=` first. That tells you whether the breakage was at initial page load, state selection, category selection, year selection, refresh, or download.
- If the page title in the diagnostics JSON is `Access Forbidden`, treat it as a site-side block on the automated browser session, not selector drift.
- If `Access Forbidden` appears at `initial_page_load` while `VAHAN_HEADLESS=true`, switch back to the default non-headless `xvfb` runtime before changing selectors.
- The metadata JSON path is included directly in the error log line, so you can open the saved HTML and screenshot without reproducing the issue live.

Example VM commands:

```bash
grep -n "failed_step=" /home/climate_dot_data/DataScraper_VahanParivahan/oem_etl_logs.txt | tail -20
find /home/climate_dot_data/DataScraper_VahanParivahan/debug_artifacts/selenium/oem -type f | tail -20
```

## Verification Checklist

- Confirm the processed CSV exists before ingestion.
- Confirm the raw OEM table schema still matches the dbt source definition.
- Confirm the curated model compiles before running it in production.
- Confirm blob upload completed before local cleanup removed files.
- If you ran the raw schema migration, confirm the migration row counts match before and after backfill.
