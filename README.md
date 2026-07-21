# ClimateDot EV Data Pipelines

This repository contains the data collection and transformation pipelines used to build ClimateDot's India EV datasets.

Today the project is operated from an Azure VM. The VM is started and stopped by external automation, but that automation and the VM configuration are not yet represented as infrastructure-as-code in this repository. This means the repo is important, but it is not yet the full source of truth for production behavior.

## Current Scope

The repo currently contains three ingestion pipelines:

- `RTO` pipeline: scrapes RTO-level data, loads raw data into SQL Server, uploads source files to Azure Blob Storage, and runs a dbt curated model.
- `OEM` pipeline: scrapes OEM data, loads raw data into SQL Server, uploads source files to Azure Blob Storage, and runs a dbt curated model.
- `State` pipeline: scrapes state-level data, loads raw data into SQL Server, uploads source files to Azure Blob Storage, and keeps a repo dbt model available for manual or future use.

## Important Operating Assumptions

- Production is currently the only real environment.
- The Azure VM can drift from the repository if changes are made directly on the machine.
- `config.yaml` is required at runtime for database and Azure Blob credentials, but is not committed here. A placeholder version lives at [`config.example.yaml`](/Users/monish/DataScraper_VahanParivahan/config.example.yaml).
- Google Chat failure alerts are optional, but if enabled the webhook secret should live in `config.yaml` or the `GOOGLE_CHAT_WEBHOOK_URL` environment variable, never in git.
- The monthly run convention is "previous month by default" unless a month and year are passed on the command line.
- Vahan source schemas can drift over time, so preprocessing should be treated as a schema boundary, not just a file conversion step.
- Vahan currently blocks headless Selenium sessions in production-like runs, so the monthly ETL scripts default to `VAHAN_HEADLESS=false` and expect `xvfb-run` to be available on the VM.

## Repository Map

- [`oem_data_etl.sh`](/Users/monish/DataScraper_VahanParivahan/oem_data_etl.sh): OEM monthly orchestration script
- [`rto_ev_data_etl.sh`](/Users/monish/DataScraper_VahanParivahan/rto_ev_data_etl.sh): RTO monthly orchestration script
- [`state_ev_data_etl.sh`](/Users/monish/DataScraper_VahanParivahan/state_ev_data_etl.sh): State monthly orchestration script
- [`rto_telangana_backfill.sh`](/Users/monish/DataScraper_VahanParivahan/rto_telangana_backfill.sh): Telangana-only historical RTO backfill wrapper
- [`oem_level`](/Users/monish/DataScraper_VahanParivahan/oem_level): OEM scraping, preprocessing, ingestion, and blob upload code
- [`rto_level`](/Users/monish/DataScraper_VahanParivahan/rto_level): RTO scraping, preprocessing, ingestion, and blob upload code
- [`state_level`](/Users/monish/DataScraper_VahanParivahan/state_level): State scraping, preprocessing, ingestion, and blob upload code
- [`climate_dot_dbt`](/Users/monish/DataScraper_VahanParivahan/climate_dot_dbt): dbt project for curated SQL models
- [`pipeline_constants.py`](/Users/monish/DataScraper_VahanParivahan/pipeline_constants.py): shared state list and shared raw fuel column mapping
- [`runtime_config.py`](/Users/monish/DataScraper_VahanParivahan/runtime_config.py): shared helpers for default month/year resolution and runtime `config.yaml` loading
- [`etl_preprocessing.py`](/Users/monish/DataScraper_VahanParivahan/etl_preprocessing.py): shared preprocessing base used by the active OEM, RTO, and State monthly pipelines
- [`etl_ingestion.py`](/Users/monish/DataScraper_VahanParivahan/etl_ingestion.py): shared CSV-to-SQL Server load base used by the active OEM, RTO, and State monthly pipelines
- [`etl_blob_upload.py`](/Users/monish/DataScraper_VahanParivahan/etl_blob_upload.py): shared blob upload wrapper used by the active OEM, RTO, and State monthly pipelines
- [`sqlserver_utils.py`](/Users/monish/DataScraper_VahanParivahan/sqlserver_utils.py): shared SQL Server connection retry helper for ingestion scripts
- [`preprocessing_schema_utils.py`](/Users/monish/DataScraper_VahanParivahan/preprocessing_schema_utils.py): shared preprocessing safeguards for schema drift
- [`blob_storage_utils.py`](/Users/monish/DataScraper_VahanParivahan/blob_storage_utils.py): shared blob container setup and upload/cleanup helpers
- [`Table and Mapping V2.xlsx`](/Users/monish/DataScraper_VahanParivahan/Table%20and%20Mapping%20V2.xlsx): mapping file used by preprocessing scripts to derive vehicle dimensions
- [`docs/architecture.md`](/Users/monish/DataScraper_VahanParivahan/docs/architecture.md): current-state architecture and operational notes
- [`docs/runbooks`](/Users/monish/DataScraper_VahanParivahan/docs/runbooks): pipeline-specific operational runbooks
- [`docs/runbooks/raw_schema_migration.md`](/Users/monish/DataScraper_VahanParivahan/docs/runbooks/raw_schema_migration.md): one-time raw schema migration runbook
- [`ops/etl_runtime.sh`](/Users/monish/DataScraper_VahanParivahan/ops/etl_runtime.sh): shared shell helper that applies the Selenium browser runtime policy
- [`ops/run_repo_checks.sh`](/Users/monish/DataScraper_VahanParivahan/ops/run_repo_checks.sh): shared repo validation entrypoint for shell checks, tests, and optional dbt parsing
- [`ops/production_vm.crontab`](/Users/monish/DataScraper_VahanParivahan/ops/production_vm.crontab): current production cron snapshot for the Azure VM
- [`sql/migrations/2026-06-19_vahan_fuel_schema_refresh.sql`](/Users/monish/DataScraper_VahanParivahan/sql/migrations/2026-06-19_vahan_fuel_schema_refresh.sql): one-time SQL Server migration for the shared raw fuel taxonomy

## Browser Runtime

The Vahan site currently behaves differently for interactive and headless browser sessions. In practice:

- the monthly ETL shell scripts default to `VAHAN_HEADLESS=false`
- Selenium-based steps are wrapped through [`ops/etl_runtime.sh`](/Users/monish/DataScraper_VahanParivahan/ops/etl_runtime.sh)
- when headless mode is disabled, Selenium steps run under `xvfb-run -a`
- `xvfb` is therefore a production VM prerequisite

If a Selenium failure writes diagnostics with page title `Access Forbidden`, treat that as a browser-session access issue first, not an immediate selector regression.

## Operational Alerts

The ETL shell scripts can now send Google Chat alerts for both failed and successful runs.

Supported webhook configuration:

- preferred in `config.yaml`:

```yaml
alerts:
  google_chat_webhook_url: https://chat.googleapis.com/v1/spaces/...
```

- or via environment variable:

```bash
export GOOGLE_CHAT_WEBHOOK_URL='https://chat.googleapis.com/v1/spaces/...'
```

Manual alert smoke test:

```bash
python3 ops/send_chat_alert.py \
  --pipeline test \
  --status TEST \
  --run-label "manual smoke test" \
  --step setup \
  --details "Google Chat webhook is configured correctly."
```

Notes:

- Failure alerts include pipeline, run label, failed step, exit code, host, browser mode, the main cron log path, and log excerpts when available.
- Success alerts send a shorter celebratory summary with emoji, the completed step, and the main cron log path.
- OEM and RTO dbt failures also include the dedicated dbt log file path in the alert details.

## Validation

The repository now has one shared validation entrypoint for both humans and CI:

```bash
bash ops/run_repo_checks.sh
```

That script currently runs:

- shell syntax checks for the monthly ETL entrypoints and shared runtime helpers
- the repository unit test suite for shared ETL support, schema drift, missing-file recovery, Selenium diagnostics, migration contracts, and Google Chat alerts

Optional dbt project parsing can be enabled when a dbt profile is available:

```bash
RUN_DBT_PARSE=1 DBT_PROFILES_DIR=/path/to/dbt/profiles bash ops/run_repo_checks.sh
```

GitHub Actions now runs the same validation flow automatically on every push and pull request. CI-specific Python dependencies live in [`requirements-ci.txt`](/Users/monish/DataScraper_VahanParivahan/requirements-ci.txt) so the fast checks do not need the full scraper runtime.

## Pipeline Summary

| Pipeline | Entry script | Raw final table | Curated dbt status | Blob upload |
| --- | --- | --- | --- | --- |
| RTO | `rto_ev_data_etl.sh` | `fact_ev_data_by_rto` | `rto_wise_ev_data` runs automatically in prod | Yes |
| OEM | `oem_data_etl.sh` | `fact_oem_data_by_state_and_category` | `oem_wise_ev_data` runs automatically in prod | Yes |
| State | `state_ev_data_etl.sh` | `fact_ev_data_by_state` | `state_wise_ev_data` exists in the repo but is not part of the current prod operating path | Yes |

Historical RTO note:

- [`rto_level/telangana_historical_backfill.py`](/Users/monish/DataScraper_VahanParivahan/rto_level/telangana_historical_backfill.py) is a Telangana-only raw backfill helper that refreshes the live Telangana RTO mapping, scrapes a historical month range, preprocesses only Telangana rows, and ingests them into `fact_ev_data_by_rto`.
- It keeps one-off backfill files isolated under [`rto_level/historical_backfill/telangana`](/Users/monish/DataScraper_VahanParivahan/rto_level/historical_backfill/telangana) instead of mixing them into the live monthly `rto_level_ev_data` tree.
- It does not automatically update the curated `rto_wise_ev_data` model unless you explicitly run it with `--run-dbt-full-refresh`.

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
- Google Chat webhook delivery is only as reliable as the configured webhook secret and network path on the VM
- Legacy raw tables on the VM may still reflect pre-migration schemas until the SQL migration is applied

This README is intended to describe the system as it exists today, not an idealized future state.
