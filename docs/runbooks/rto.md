# RTO Runbook

## Purpose

The RTO pipeline collects monthly EV registration data at the RTO-office grain, loads it into SQL Server raw tables, uploads artifacts to Azure Blob Storage, and builds the curated `rto_wise_ev_data` dbt model.

## Entry Point

- Shell script: [`rto_ev_data_etl.sh`](/Users/monish/DataScraper_VahanParivahan/rto_ev_data_etl.sh)

## Current Flow

1. `rto_level/rto_level_data_scraper.py`
2. Refresh `output.json` with the latest state-to-RTO mapping, while falling back to the previous file for any states that fail to refresh
3. `rto_level/rto_level_get_missing_files.py`
4. `rto_level/rto_level_data_pre_processing.py`
5. `rto_level/rto_level_data_ingestion.py`
6. `rto_level/upload_files_to_blob_storage.py`
7. `dbt run --select rto_wise_ev_data`

## Default Execution Behavior

- If no arguments are provided, the script targets the previous calendar month.
- If arguments are provided, they should be passed as `MON YEAR`, for example `OCT 2024`.

## Important Files

- Preprocessing: [`rto_level/rto_level_data_pre_processing.py`](/Users/monish/DataScraper_VahanParivahan/rto_level/rto_level_data_pre_processing.py)
- Ingestion: [`rto_level/rto_level_data_ingestion.py`](/Users/monish/DataScraper_VahanParivahan/rto_level/rto_level_data_ingestion.py)
- Blob upload: [`rto_level/upload_files_to_blob_storage.py`](/Users/monish/DataScraper_VahanParivahan/rto_level/upload_files_to_blob_storage.py)
- Curated model: [`climate_dot_dbt/models/curated/rto_wise_ev_data.sql`](/Users/monish/DataScraper_VahanParivahan/climate_dot_dbt/models/curated/rto_wise_ev_data.sql)
- Shared constants: [`pipeline_constants.py`](/Users/monish/DataScraper_VahanParivahan/pipeline_constants.py)
- Shared schema helpers: [`preprocessing_schema_utils.py`](/Users/monish/DataScraper_VahanParivahan/preprocessing_schema_utils.py)

## Files and Tables Touched

- Downloaded XLSX files: `rto_level/rto_level_ev_data/<state>/<rto_name>_<rto_code>/<year>/<month>/reportTable.xlsx`
- Processed CSV: `rto_level_ev_data_<MON>_<YEAR>.csv`
- Staging table: `staging_fact_ev_data_by_rto`
- Raw final table: `fact_ev_data_by_rto`
- Curated model target: `rto_wise_ev_data`

## Manual Commands

End-to-end script:

```bash
./rto_ev_data_etl.sh
```

Specific month:

```bash
./rto_ev_data_etl.sh OCT 2024
```

dbt only:

```bash
cd /home/climate_dot_data/DataScraper_VahanParivahan/climate_dot_dbt
dbt parse
dbt compile --select rto_wise_ev_data
dbt run --select rto_wise_ev_data
```

## Operational Notes

- The preprocessing step derives `rto_name` and `rto_code` from the folder name.
- The entrypoint now refreshes the RTO mapping on every run and writes the merged result back to `output.json`.
- If a live mapping refresh is partial, the scraper falls back to the previous `output.json` for the missing states instead of silently dropping them.
- If a state is still missing after the merge, the scraper now fails early with a clear missing-state error before download work starts.
- The preprocessing step uses the shared fuel taxonomy and logs unexpected raw columns.
- Missing expected output columns are written as `NULL`, not `0`.
- The ingestion step deletes matching rows from the raw final table before inserting staged data.
- The upload step removes local XLSX directories after blob upload and deletes the processed CSV after uploading it.
- The dbt model enriches raw rows with district via `rto_code_to_district_mapping`.
- If the raw SQL tables have not been migrated yet, apply the raw schema migration before relying on the newer shared fuel columns.

## Common Failure Points

- `config.yaml` missing or stale on the VM
- Partial scrape output causing missing XLSX files
- Folder naming drift that breaks `rto_name` and `rto_code` extraction
- dbt duplicate model paths on the VM if stale model folders remain under `climate_dot_dbt/models`

## Selenium Failure Triage

- Every Selenium lookup now logs a semantic `failed_step`, not just a raw stacktrace.
- Retry logs now include the full run context, for example state, RTO, month, and year.
- On each failed lookup, the scraper writes three debug artifacts under `debug_artifacts/selenium/rto/`:
  - a screenshot `.png`
  - the page source `.html`
  - a metadata `.json` file with step name, locator, URL, and run context
- If a run fails, search the log for `failed_step=` first. That tells you whether the breakage was at initial page load, state selection, RTO selection, year selection, refresh, or download.
- The metadata JSON path is included directly in the error log line, so you can open the saved HTML and screenshot without reproducing the issue live.

Example VM commands:

```bash
grep -n "failed_step=" /home/climate_dot_data/DataScraper_VahanParivahan/rto_ev_etl_logs.txt | tail -20
find /home/climate_dot_data/DataScraper_VahanParivahan/debug_artifacts/selenium/rto -type f | tail -20
```

## Verification Checklist

- Confirm the processed CSV exists before ingestion.
- Confirm raw table load completes without SQL errors.
- Confirm blob upload completed before local cleanup removed files.
- Confirm `dbt run --select rto_wise_ev_data` succeeds after any repo sync.
- If you ran the raw schema migration, confirm the migration row counts match before and after backfill.
