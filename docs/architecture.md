# Architecture

## Overview

This project collects monthly EV registration data from Vahan-derived report downloads at three grains:

- `RTO`
- `OEM`
- `State`

Each pipeline follows roughly the same high-level shape:

1. Scrape XLSX reports into a local folder structure on the VM.
2. Re-run a "missing files" pass to fill obvious gaps.
3. Preprocess downloaded XLSX files into a monthly CSV.
4. Load the CSV into a SQL Server staging table.
5. Delete matching records from the corresponding raw fact table.
6. Insert staged rows into the raw fact table.
7. Upload downloaded XLSX files and the processed CSV to Azure Blob Storage.
8. Remove local downloaded files after successful blob upload.

Only the `RTO` orchestration script currently runs dbt automatically at the end of the shell script. `OEM` and `State` have curated dbt models in the repo, but they are not yet wired into their shell scripts.

The active preprocessing scripts now share:

- a centralized state list in [`pipeline_constants.py`](/Users/monish/DataScraper_VahanParivahan/pipeline_constants.py)
- a shared raw fuel column mapping in [`pipeline_constants.py`](/Users/monish/DataScraper_VahanParivahan/pipeline_constants.py)
- shared schema-drift safeguards in [`preprocessing_schema_utils.py`](/Users/monish/DataScraper_VahanParivahan/preprocessing_schema_utils.py)

## Runtime Environment

The production runtime is an Azure VM started and stopped by external automation. The automation mechanism is not represented in this repository today.

Important consequences:

- The repo is not the only source of truth.
- Changes made directly on the VM can survive independently from git history.
- dbt parsing issues can occur if stale model directories remain on disk.
- Any production change should assume VM drift is possible until proven otherwise.

## Code Layout

- [`OEM-level`](/Users/monish/DataScraper_VahanParivahan/OEM-level): OEM pipeline code
- [`rto_level`](/Users/monish/DataScraper_VahanParivahan/rto_level): RTO pipeline code
- [`State-level`](/Users/monish/DataScraper_VahanParivahan/State-level): State pipeline code
- [`climate_dot_dbt`](/Users/monish/DataScraper_VahanParivahan/climate_dot_dbt): curated SQL layer

## Monthly Orchestration Scripts

- [`oem_data_etl.sh`](/Users/monish/DataScraper_VahanParivahan/oem_data_etl.sh)
- [`rto_ev_data_etl.sh`](/Users/monish/DataScraper_VahanParivahan/rto_ev_data_etl.sh)
- [`state_ev_data_etl.sh`](/Users/monish/DataScraper_VahanParivahan/state_ev_data_etl.sh)

All three scripts:

- activate the project virtual environment at `/home/climate_dot_data/DataScraper_VahanParivahan/venv`
- change into `/home/climate_dot_data/DataScraper_VahanParivahan`
- accept optional `MONTH YEAR` arguments
- default to the previous calendar month if no arguments are passed

## Data Flow By Pipeline

### RTO

- Raw downloaded files live under `rto_level/rto_level_ev_data/...`
- Processed monthly CSV is `rto_level_ev_data_<MON>_<YEAR>.csv`
- Staging table is `staging_fact_ev_data_by_rto`
- Raw final table is `fact_ev_data_by_rto`
- Curated dbt model is `rto_wise_ev_data`

### OEM

- Raw downloaded files live under `OEM-level/oem_data_by_state_and_category/...`
- Processed monthly CSV is `oem_data_by_state_and_category_<MON>_<YEAR>.csv`
- Staging table is `staging_fact_oem_data_by_state_and_category`
- Raw final table is `fact_oem_data_by_state_and_category`
- Curated dbt model is `oem_wise_ev_data`

### State

- Raw downloaded files live under `State-level/state_level_ev_data/...`
- Processed monthly CSV is `state_level_ev_data_<MON>_<YEAR>.csv`
- Staging table is `staging_fact_ev_data_by_state`
- Raw final table is `fact_ev_data_by_state`
- Curated dbt model is `state_wise_ev_data`

## dbt Layer

Curated dbt models live under [`climate_dot_dbt/models/curated`](/Users/monish/DataScraper_VahanParivahan/climate_dot_dbt/models/curated).

Current models:

- `rto_wise_ev_data`
- `oem_wise_ev_data`
- `state_wise_ev_data`

Source definitions live in [`sources.yml`](/Users/monish/DataScraper_VahanParivahan/climate_dot_dbt/models/curated/sources.yml).

At the time of writing:

- The folder name was cleaned up from `climate_dot_gpt` to `curated`.
- The repo does not yet enforce a separate curated schema in `dbt_project.yml`.
- Schema resolution still depends on the active dbt profile on the VM.
- The curated models now read the newer raw fuel taxonomy directly instead of forcing new Vahan columns back into legacy names.

## Schema Drift Handling

The raw Vahan exports should be treated as unstable upstream contracts.

Current behavior in the active preprocessing scripts:

- unexpected source columns are logged as warnings
- missing expected output columns are created as `NULL`
- missing columns are not defaulted to `0`
- shared fuel naming is kept consistent across `RTO`, `State`, and `OEM`

This makes the pipelines more resilient to raw-file changes while still surfacing drift in logs.

## Raw Schema Migration

The raw SQL Server tables still need an explicit migration before the full shared schema exists in production.

Relevant files:

- SQL migration: [`sql/migrations/2026-06-19_vahan_fuel_schema_refresh.sql`](/Users/monish/DataScraper_VahanParivahan/sql/migrations/2026-06-19_vahan_fuel_schema_refresh.sql)
- Runbook: [`docs/runbooks/raw_schema_migration.md`](/Users/monish/DataScraper_VahanParivahan/docs/runbooks/raw_schema_migration.md)

That migration:

- backs up the existing raw fact tables
- recreates staging and raw tables with the new schema
- backfills historical values where a safe mapping exists
- keeps unmappable legacy values as `NULL`
- standardizes metadata columns on `inserted_at`

## Mapping File Dependency

The preprocessing layer depends on [`Table and Mapping V2.xlsx`](/Users/monish/DataScraper_VahanParivahan/Table%20and%20Mapping%20V2.xlsx), specifically the `Mapping` sheet, to derive:

- `vehicle_type`
- `vehicle_category`
- `vehicle_use_type`

This file is operationally important and should be treated like code, even though it is an Excel asset.

## Blob Storage Behavior

Each pipeline uploads two classes of artifacts:

- raw XLSX report files into a container-specific folder structure
- processed monthly CSV files into a container-specific CSV/raw container

After upload, the scripts remove local files and directories. That means reruns can depend on whether the scrape step recreates the needed local structure.

## Known Inconsistencies

- `RTO` shell orchestration runs dbt automatically, while `OEM` and `State` do not.
- Legacy raw tables on the VM may still be on the pre-migration schema until the SQL migration is applied.
- The SQL used in dbt models is not fully standardized yet across `RTO`, `OEM`, and `State`.
- VM contents can drift from the repository, especially around dbt model directories.

These are good cleanup targets, but they are documented here first so future changes are explicit and reviewable.
