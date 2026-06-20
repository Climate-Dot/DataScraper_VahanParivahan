# Raw Schema Migration Runbook

## Purpose

This runbook is for the one-time raw table migration that aligns the `RTO`, `State`, and `OEM` pipelines to the newer Vahan fuel taxonomy without aliasing new source columns back into legacy names.

## Migration File

- SQL script: [`sql/migrations/2026-06-19_vahan_fuel_schema_refresh.sql`](/Users/monish/DataScraper_VahanParivahan/sql/migrations/2026-06-19_vahan_fuel_schema_refresh.sql)

## What The Migration Does

1. Creates one-time backup tables for the three existing raw fact tables.
2. Drops and recreates the three staging tables with the new shared schema.
3. Drops and recreates the three raw fact tables with the new shared schema.
4. Backfills historical data from the backups into the new raw tables.
5. Preserves exact-match newer columns when they already exist in the backup tables.
6. Leaves genuinely unmappable legacy values as `NULL` instead of forcing `0`.

## Safety Notes

- The migration creates backup tables first and refuses to overwrite them if they already exist.
- The drop, recreate, and backfill steps run inside a transaction.
- The script is tolerant of VM drift for some legacy columns such as `electric_vehicles` vs `electric_bov` and `insert_date` vs `inserted_at`.
- Legacy columns like `petrol_ethanol` are not remapped into the new E20 columns.

## Execution Order

1. Sync the repo to the VM and verify the target branch is the one you expect.
2. Open the migration SQL file and sanity-check the backup table names include the intended date suffix.
3. Run the migration in SQL Server against production.
4. Review the six row-count results returned by the script.
5. Run a full refresh for the three curated dbt models.
6. Run one known month manually if you want an extra confidence pass before the next scheduled cron run.

## Commands After Raw Migration

From the dbt project directory on the VM:

```bash
cd /home/climate_dot_data/DataScraper_VahanParivahan/climate_dot_dbt
dbt parse
dbt run --full-refresh --select rto_wise_ev_data state_wise_ev_data oem_wise_ev_data
```

## What To Verify

- Backup tables exist:
  - `dbo.fact_ev_data_by_rto_backup_20260619`
  - `dbo.fact_ev_data_by_state_backup_20260619`
  - `dbo.fact_oem_data_by_state_and_category_backup_20260619`
- New raw tables contain rows after backfill.
- `inserted_at` exists on all new staging and raw tables.
- The curated dbt models run successfully with `--full-refresh`.

## Expected Functional Behavior After Migration

- The active preprocessing scripts keep the new Vahan columns as new columns.
- Missing expected output columns are written as `NULL`, not `0`.
- Unexpected future raw columns are logged so schema drift is visible in job logs.
