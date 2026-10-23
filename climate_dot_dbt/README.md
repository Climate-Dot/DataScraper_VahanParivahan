# climate_dot_dbt

This dbt project contains the curated SQL models that sit on top of the raw SQL Server tables loaded by the scraper pipelines.

Current operating notes:

- [`models/curated`](/Users/monish/DataScraper_VahanParivahan/climate_dot_dbt/models/curated) holds the active EV models.
- `rto_wise_ev_data` and `oem_wise_ev_data` are part of the live production workflow today.
- `state_wise_ev_data` remains available for manual validation and future production use, but it is not currently run automatically from the monthly VM cron path.
- Materialization is defined in each model so incremental behavior is visible at the SQL layer.
- All three models set `on_schema_change='append_new_columns'`. The dbt 1.8 default is
  `ignore`, which adds a new column to the SELECT while silently leaving it off the
  existing table and still reporting success. Note that `append_new_columns` adds the
  column but does **not** backfill existing rows, so a schema addition still needs one
  `dbt run --full-refresh --select <model>` to populate history.

## Fuel columns: two header generations

Vahan renamed two fuel-column headers effective May 2026:

| Vahan header (reports <= Apr 2026) | Vahan header (reports >= May 2026) | curated column |
|---|---|---|
| `ETHANOL` | - | `ethanol` |
| - | `ETHANOL(E100)` | `ethanol_e100` |
| `PETROL/ETHANOL` | - | `petrol_ethanol` |
| - | `PETROL(E20)` | `petrol_e20` |

Both generations are loaded, into **separate** columns. They are deliberately not
merged: treating `PETROL/ETHANOL` as equivalent to `PETROL(E20)` is an inference the
source does not support, and the `2026-06-21` repair migration explicitly reversed an
earlier attempt to carry legacy values into the newer semantic fields. Whether to
combine them is a business decision, to be made with both columns visible.

Consequences for anyone querying these models:

- `total` is authoritative and always has been.
- `electric_bov` and `pure_ev` are unaffected and have always been correct.
- In `rto_wise_ev_data`, the fuel columns now sum exactly to `total` in every year
  (verified 2013-2026, gap 0).
- In `oem_wise_ev_data` and `state_wise_ev_data` the legacy columns exist but their
  pre-May-2026 history has **not** been reprocessed yet, so summing their fuel columns
  still falls short of `total` by roughly 20% in recent years. Use `total` for those.

The physical column order of the raw tables, the column order in `sources.yml`, and
`COMMON_FUEL_COLUMN_RENAME_MAP` are all kept in the same order deliberately
(`[dimensions..., *map.values(), inserted_at]`), and `tests/test_dbt_contracts.py`
enforces the `sources.yml` half of that. When generating DDL from an existing table,
read the order from `INFORMATION_SCHEMA.COLUMNS`, never from `sources.yml`.

Safe validation commands:

```bash
bash /Users/monish/DataScraper_VahanParivahan/ops/run_repo_checks.sh
RUN_DBT_PARSE=1 DBT_PROFILES_DIR=/path/to/dbt/profiles bash /Users/monish/DataScraper_VahanParivahan/ops/run_repo_checks.sh
```
