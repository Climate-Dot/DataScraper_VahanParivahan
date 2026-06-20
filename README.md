# ClimateDot EV Data Pipelines

This repository contains the data collection and transformation pipelines used to build ClimateDot's India EV datasets.

Today the project is operated from an Azure VM. The VM is started and stopped by external automation, but that automation and the VM configuration are not yet represented as infrastructure-as-code in this repository. This means the repo is important, but it is not yet the full source of truth for production behavior.

## Current Scope

The repo currently contains three ingestion pipelines:

- `RTO` pipeline: scrapes RTO-level data, loads raw data into SQL Server, uploads source files to Azure Blob Storage, and runs a dbt curated model.
- `OEM` pipeline: scrapes OEM-level data, loads raw data into SQL Server, uploads source files to Azure Blob Storage, and has a dbt curated model available for manual execution.
- `State` pipeline: scrapes state-level data, loads raw data into SQL Server, uploads source files to Azure Blob Storage, and keeps a repo dbt model available for manual or future use.

## Important Operating Assumptions

- Production is currently the only real environment.
- The Azure VM can drift from the repository if changes are made directly on the machine.
- `config.yaml` is required at runtime for database and Azure Blob credentials, but is not committed here.
- The monthly run convention is "previous month by default" unless a month and year are passed on the command line.
- Vahan source schemas can drift over time, so preprocessing should be treated as a schema boundary, not just a file conversion step.

## Repository Map

- [`oem_data_etl.sh`](/Users/monish/DataScraper_VahanParivahan/oem_data_etl.sh): OEM monthly orchestration script
- [`rto_ev_data_etl.sh`](/Users/monish/DataScraper_VahanParivahan/rto_ev_data_etl.sh): RTO monthly orchestration script
- [`state_ev_data_etl.sh`](/Users/monish/DataScraper_VahanParivahan/state_ev_data_etl.sh): State monthly orchestration script
- [`OEM-level`](/Users/monish/DataScraper_VahanParivahan/OEM-level): OEM scraping, preprocessing, ingestion, and blob upload code
- [`rto_level`](/Users/monish/DataScraper_VahanParivahan/rto_level): RTO scraping, preprocessing, ingestion, and blob upload code
- [`State-level`](/Users/monish/DataScraper_VahanParivahan/State-level): State scraping, preprocessing, ingestion, and blob upload code
- [`climate_dot_dbt`](/Users/monish/DataScraper_VahanParivahan/climate_dot_dbt): dbt project for curated SQL models
- [`pipeline_constants.py`](/Users/monish/DataScraper_VahanParivahan/pipeline_constants.py): shared state list and shared raw fuel column mapping
- [`preprocessing_schema_utils.py`](/Users/monish/DataScraper_VahanParivahan/preprocessing_schema_utils.py): shared preprocessing safeguards for schema drift
- [`Table and Mapping V2.xlsx`](/Users/monish/DataScraper_VahanParivahan/Table%20and%20Mapping%20V2.xlsx): mapping file used by preprocessing scripts to derive vehicle dimensions
- [`docs/architecture.md`](/Users/monish/DataScraper_VahanParivahan/docs/architecture.md): current-state architecture and operational notes
- [`docs/runbooks`](/Users/monish/DataScraper_VahanParivahan/docs/runbooks): pipeline-specific operational runbooks
- [`docs/runbooks/raw_schema_migration.md`](/Users/monish/DataScraper_VahanParivahan/docs/runbooks/raw_schema_migration.md): one-time raw schema migration runbook
- [`sql/migrations/2026-06-19_vahan_fuel_schema_refresh.sql`](/Users/monish/DataScraper_VahanParivahan/sql/migrations/2026-06-19_vahan_fuel_schema_refresh.sql): one-time SQL Server migration for the shared raw fuel taxonomy

## Pipeline Summary

| Pipeline | Entry script | Raw final table | Curated dbt status | Blob upload |
| --- | --- | --- | --- | --- |
| RTO | `rto_ev_data_etl.sh` | `fact_ev_data_by_rto` | `rto_wise_ev_data` runs automatically in prod | Yes |
| OEM | `oem_data_etl.sh` | `fact_oem_data_by_state_and_category` | `oem_wise_ev_data` exists and is refreshed manually in prod | Yes |
| State | `state_ev_data_etl.sh` | `fact_ev_data_by_state` | `state_wise_ev_data` exists in the repo but is not part of the current prod operating path | Yes |

## dbt Project

The dbt models now live under [`climate_dot_dbt/models/curated`](/Users/monish/DataScraper_VahanParivahan/climate_dot_dbt/models/curated). The repository does not yet set an explicit dbt target schema in `dbt_project.yml`, so schema resolution still depends on the active dbt profile on the VM.

The active curated models now read from the newer shared raw fuel taxonomy directly. They do not alias new raw columns like `PETROL(E20)` back into legacy columns such as `petrol_ethanol`.

Current curated models:

- [`rto_wise_ev_data.sql`](/Users/monish/DataScraper_VahanParivahan/climate_dot_dbt/models/curated/rto_wise_ev_data.sql)
- [`oem_wise_ev_data.sql`](/Users/monish/DataScraper_VahanParivahan/climate_dot_dbt/models/curated/oem_wise_ev_data.sql)
- [`state_wise_ev_data.sql`](/Users/monish/DataScraper_VahanParivahan/climate_dot_dbt/models/curated/state_wise_ev_data.sql)

Operational note:

- Only `rto_wise_ev_data` and `oem_wise_ev_data` are currently used in production.
- `state_wise_ev_data` remains in the repo for manual or future use, but it is not currently materialized as part of the live prod workflow.

## Recommended Reading Order

If you are onboarding yourself back into this project, read in this order:

1. [`docs/architecture.md`](/Users/monish/DataScraper_VahanParivahan/docs/architecture.md)
2. The relevant pipeline runbook under [`docs/runbooks`](/Users/monish/DataScraper_VahanParivahan/docs/runbooks)
3. The pipeline shell script
4. The preprocessing and ingestion scripts
5. The matching dbt model in [`climate_dot_dbt/models/curated`](/Users/monish/DataScraper_VahanParivahan/climate_dot_dbt/models/curated)

## Known Gaps

- No infrastructure-as-code for the Azure VM lifecycle
- No committed environment/profile documentation for dbt targets
- No lower environment for safe end-to-end testing
- Legacy raw tables on the VM may still reflect pre-migration schemas until the SQL migration is applied
- Example dbt starter models still exist under [`climate_dot_dbt/models/example`](/Users/monish/DataScraper_VahanParivahan/climate_dot_dbt/models/example)

This README is intended to describe the system as it exists today, not an idealized future state.
