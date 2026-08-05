# RTO Code → District Mapping Refresh (2026-08-05)

## Purpose

One-time refresh of `dbo.rto_code_to_district_mapping`, the lookup table joined
into `rto_wise_ev_data` (`climate_dot_dbt/models/curated/rto_wise_ev_data.sql`)
to attach a district to each RTO office. The maintainer supplied an updated
mapping spreadsheet ("[Updated] RTO mapping for EV Dashboard 2.0.xlsx", sheet
"List of RTOs | CD database") with more current district assignments than the
existing table.

## Why SCD1, not SCD2

The table is a flat `rto_code -> district` lookup with no temporal dimension,
and the dbt join (`ON f.rto_code = d.rto_code`) has no date logic. Introducing
SCD2 (effective-dated rows) would require redesigning that join to be
date-aware, which nothing here calls for. A straight overwrite-with-backup
(SCD1) matches how the table is already consumed and mirrors the existing
backup convention in
[`sql/migrations/2026-06-19_vahan_fuel_schema_refresh.sql`](../../sql/migrations/2026-06-19_vahan_fuel_schema_refresh.sql).

## What Changed

- The live table had **1,455 existing rows** (not empty, contrary to initial
  assumption — verified via `INFORMATION_SCHEMA.COLUMNS` / `COUNT(*)` before
  touching anything).
- Backed up to `dbo.rto_code_to_district_mapping_backup_20260805` (refuses to
  run if that backup name already exists).
- Table dropped and recreated with the identical 2-column contract
  (`rto_code`, `district`, both `NVARCHAR(MAX)`) already documented in
  [`sources.yml`](../../climate_dot_dbt/models/curated/sources.yml) — no dbt
  model or source-doc changes needed.
- Reloaded from
  [`sql/migrations/2026-08-05_rto_code_to_district_mapping_refresh.csv`](../../sql/migrations/2026-08-05_rto_code_to_district_mapping_refresh.csv),
  a cleaned extract of the "List of RTOs | CD database" sheet: 1,661 rows.

## Data Cleaning Applied

The source sheet had 1,670 rows. 9 were excluded because their "Code" column
was a placeholder like `null98`, `null100`-`null105`, `null62` rather than a
real Vahan RTO code:

- `null98` was reused for two different offices (Meghalaya "Commissioner of
  Transport" and West Bengal "CANNING ARTO") — loading it as a real key would
  either collide or misattribute one office's district to the other.
- The other 8 placeholders are one-off unknowns (mostly Haryana offices with
  no resolved code yet).

Per this repo's existing rule that unmappable values stay `NULL` rather than
being fabricated, these 9 rows are simply not loaded — the affected offices
remain unmapped, same as before this refresh (no regression, just an honest
"still unknown").

Separately, 249 rows have a real `rto_code` but the sheet's "Mapping Status"
is "Data not available" (no district determined yet). Those are loaded with
`district = NULL`, not dropped, since the code itself is real and worth
keeping for when a district is eventually resolved.

Net: 1,661 rows loaded, 1,412 with a real district, 249 with `district IS
NULL`.

## Execution

```bash
# 1. Run the backup + recreate DDL (from sql/migrations/2026-08-05_rto_code_to_district_mapping_refresh.sql)
#    against production SQL Server.
# 2. Bulk-load sql/migrations/2026-08-05_rto_code_to_district_mapping_refresh.csv
#    into the freshly recreated dbo.rto_code_to_district_mapping.
```

## Rollback

`dbo.rto_code_to_district_mapping_backup_20260805` holds the exact prior
1,455-row table. To roll back: drop the new table and rename the backup back
to `dbo.rto_code_to_district_mapping`.

## Downstream Refresh

`rto_wise_ev_data` is an incremental dbt model — a normal `dbt run` will not
re-materialize historical rows with the new district values. This refresh was
intentionally timed to land right before the full-refresh already planned at
the end of the 2013-2026 historical RTO remediation
([`project-rto-selector-bug-and-remediation.md`](../../../.claude/projects/-Users-monish-DataScraper-VahanParivahan/memory/project-rto-selector-bug-and-remediation.md)),
so `dbt run --select rto_wise_ev_data --full-refresh` only needs to run once
to pick up both changes.
