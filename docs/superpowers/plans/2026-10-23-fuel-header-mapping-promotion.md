# Fuel Header Mapping Promotion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Recover the ~18–22% of historical vehicle registrations that are stored in no fuel column, by loading Vahan's pre-May-2026 fuel headers into two new first-class columns across the RTO, OEM and State pipelines.

**Architecture:** Vahan renamed two fuel headers effective May 2026. Only the new spellings are in the shared rename map, so for reports dated April 2026 and earlier those two columns were dropped at preprocessing. The legacy headers get their **own** output columns (`ethanol`, `petrol_ethanol`) rather than being folded into `ethanol_e100`/`petrol_e20`, because equivalence is an unverified inference and a prior migration that assumed it was explicitly reverted. Schema and mapping changes land first (both backward compatible). History is then recovered by **updating only the two new columns in place** from a verified parallel table — not by swapping tables — which preserves `inserted_at` and every other column byte-for-byte, inserts no rows, and is reversible by setting the two columns back to `NULL`.

**Current state:** The RTO parallel table `fact_ev_data_by_rto_fuelfix` is already being populated by `rto_level/fuel_mapping_fix/reprocess_to_fuelfix.py`, which uses a one-off preprocessing subclass. Once Task 2 lands, production preprocessing emits the same two columns and that subclass becomes redundant.

**Tech Stack:** Python 3.12, pandas, openpyxl, pyodbc, Azure SQL (General Purpose Serverless), dbt 1.8.9 (dbt-sqlserver), unittest.

**Spec:** No separate spec document exists; this plan carries its own Problem section below. The durable supporting artifacts are the module docstring in `rto_level/fuel_mapping_fix/rto_preprocessing_fuelfix.py` and the migration header in `sql/migrations/2026-10-23_rto_fuelfix_parallel_tables.sql`.

---

## AS BUILT — executed 2026-10-23. Read this before the plan below.

This plan was carried out, but **not as written**. The body below is preserved as
the original reasoning; where the two disagree, this block is what happened. A
code review flagged that an unticked plan prescribing the opposite of the shipped
work would mislead anyone executing it fresh.

**Deviations, all deliberate:**

1. **Task 1 did a full table rebuild, not `ALTER TABLE ADD`.** The plan argued a
   rebuild buys "nothing but column cosmetics". That was wrong: the
   staging-to-final transfer is `INSERT INTO final SELECT * FROM staging`, which
   is positional, so physical column order is a correctness surface. `ALTER`
   would also have left `inserted_at` stranded mid-table and the six tables
   permanently out of order with `sources.yml` and the shared map. The rebuild
   was measured at ~6 minutes total and was gated per table on a `CHECKSUM_AGG`
   fingerprint of every original column, renaming only on an exact match.

2. **Task 1's contract test was not written.** The migration file turned out to
   be a record of work already done rather than a runnable script, so a test
   asserting its SQL text had little value. It carries an explicit
   "NOT A RUNNABLE SCRIPT" banner instead.

3. **Task 2's `_drop_duplicate_legacy_headers` was not implemented.** Mapping the
   two legacy headers to their *own* output columns removed the failure mode it
   guarded against: two source headers can no longer collide on one output name.
   The residual risk changed shape rather than vanishing — a single file carrying
   both spellings would now populate both columns and push the fuel sum above
   `total` — and production evidence says that never happens (row-level
   `total <> sum(fuels)` count is 0 across 3.4M rows).

4. **Task 2's five behavioural tests were not written.** Presence of the columns
   is covered because the existing tests derive expectations from the map, and
   the model wiring is covered by `test_dbt_contracts`. No test asserts a *value*
   lands in `petrol_ethanol`; that remains a real, if low-risk, gap.

5. **Promotion backfilled 8 columns for OEM, not 2.** Reprocessing surfaced a
   second, unrelated defect: six newer fuel columns were 0 for 2023-2026 because
   those months predate the 2026-06-19 schema refresh. Their 567,810 vehicles
   were exactly OEM's residual gap.

6. **A missing month was inserted.** OEM had no September 2024 at all. 7,525
   rows / 1,725,189 registrations / 159,425 EVs were added from the reprocess.
   This is the only INSERT of the day and the only intended change to any EV
   total. Why the month was never loaded is NOT investigated.

7. **OEM history went back to 2013, not 2023.** OEM's legacy `PETROL/ETHANOL`
   column carries data as far back as 2014, unlike RTO where pre-2023 was
   effectively empty.

8. **State was dropped from scope** by decision, after measurement showed 21 of
   its 40 relevant months have no raw files on disk or in blob (coverage starts
   2024-10), making ~852,702 vehicles unrecoverable without re-scraping.

9. **File names differ.** Everything is dated `2026-10-23`, not `2026-10-24`, and
   the backup token is `_prerebuild_backup_20261023`. Task 7's
   `docs/runbooks/fuel_header_mapping_cutover.md` was not written; the migration
   file and `climate_dot_dbt/README.md` carry that content instead.

**Outcome:** `rto_wise_ev_data` and `oem_wise_ev_data` now reconcile exactly —
the fuel columns sum to `total` with gap 0 in every year 2013-2026, at row level
not merely in aggregate. EV counts are bit-identical to the pre-change backups
except OEM's deliberate +159,425. `state_wise_ev_data` is unchanged and still
~20% short.

---

## Problem

Measured on the live `climate_dot` database, 2026-10-23:

| Vahan header | present | maps to | in shared map? |
|---|---|---|---|
| `ETHANOL` | reports ≤ Apr 2026 | nothing — dropped | no |
| `ETHANOL(E100)` | reports ≥ May 2026 | `ethanol_e100` | yes |
| `PETROL/ETHANOL` | reports ≤ Apr 2026 | nothing — dropped | no |
| `PETROL(E20)` | reports ≥ May 2026 | `petrol_e20` | yes |

Unattributed share of `total` (2025, raw tables): RTO **17.54%**, OEM **21.87%**, State **21.87%**. By month for RTO the shortfall rises from ~2% (Jan 2025) to ~40% (Apr 2026), then drops to **0.00%** from May 2026. It also varies **0%–27% by state**, because states re-scraped after the rename already carry the new spellings — which makes any pre-May-2026 cross-state fuel comparison invalid today.

Verified non-issues: `electric_bov`, `pure_ev` and `total` are never NULL, so no EV figure is affected. The two spellings never co-occur in one file and are complementary by row, so nothing is double-counted.

**Exhaustive header sweep (29,182 files — every office in one full month per year, 2013–2026, zero read errors):** 42 distinct headers exist in total. `ETHANOL` (20,158 files) and `PETROL/ETHANOL` (20,158 files) are the **only** unmapped fuel headers; every other mapped header was seen and nothing else is unaccounted for. The remaining three are structural, not fuels:

| header | files | what it is |
|---|---|---|
| `Unnamed: 1` | 24,670 | `vehicle_class`, mapped in each pipeline's own map, not the shared fuel map |
| `Unnamed: 26` | 15,292 | Total in **27-column** reports |
| `Unnamed: 36` | 1,216 | Total in **37-column** reports |
| `Unnamed: 38` | 8,162 | Total in **39-column** reports — the canonical mapping |

So reports come in three widths and the Total column is positional. `BaseExcelPreprocessor._normalize_report_total_column` already handles this by taking the final column when the canonical `Unnamed: 38` is absent, excluding `Unnamed: 1`. Confirmed in data: `total` is non-NULL on every reprocessed row and the fuel columns reconcile to it exactly in every year.

**EV safety confirmed against production** over the first 94 reprocessed months (2013-01 → 2020-10): row counts and `electric_bov + pure_ev` are **identical**, difference +0.0000%.

Proven on one month (JUN 2025, via the parallel table): `petrol_ethanol` recovers 359,689 vehicles, `petrol_e20` stays at 63,595 unchanged, and the gap closes from 17.21% to 0.00%.

## Global Constraints

- Legacy headers map to their **own** columns. Never alias `ethanol` → `ethanol_e100` or `petrol_ethanol` → `petrol_e20`. Merging them is a later business decision.
- Missing fuel values stay `NULL`, never `0`.
- Reports exist in three widths (27, 37 and 39 columns) and the Total column is unnamed and positional (`Unnamed: 26`, `Unnamed: 36`, `Unnamed: 38`). Any change to `_normalize_report_total_column` must keep working for all three; it currently takes the final column when the canonical name is absent, excluding `Unnamed: 1`.
- **Deletes are permitted only against the `*_fuelfix` tables.** Production tables are never deleted from as part of this work; promotion is an `UPDATE` of two columns.
- Raw and staging tables are `nvarchar(MAX)`; staging→final transfer is `INSERT INTO final SELECT * FROM staging`, which is **positional**, so each raw/staging pair must receive new columns at the same ordinal position.
- New columns are appended by `ALTER TABLE ADD` in the **same order** to each raw/staging pair, so their ordinal positions match. That is the only ordering requirement: the staging-to-final transfer is `SELECT *` and therefore positional, while every other consumer is name-based.
- Appending leaves the physical order as `..., total, inserted_at, ethanol, petrol_ethanol`, so `inserted_at` is **no longer the last column**. This is intentional, not an oversight: the alternative is rewriting ~3.5M rows across six production tables to gain column cosmetics, on data whose raw source files cannot be re-downloaded once the Vahan portal closes. Nothing in the codebase reads a column by position.
- `sources.yml` lists columns in `COMMON_FUEL_COLUMN_RENAME_MAP.values()` order with `inserted_at` last, which is a documentation contract checked by `tests/test_dbt_contracts.py` and is deliberately **not** the physical table order. When generating DDL from an existing table — a pattern this repo uses so types cannot drift — read the physical order from `INFORMATION_SCHEMA.COLUMNS`, never from `sources.yml`.
- `COMMON_FUEL_COLUMN_RENAME_MAP` is shared by RTO, OEM and State preprocessing. Changing it changes all three; all six tables move together.
- Every migration carries a backup table and rollback notes.
- `inserted_at` must be unchanged on every pre-existing row. Production currently holds 5 distinct `inserted_at` days (2026-08-08 to 2026-10-02); a table swap would replace all of them with the reprocess timestamp, which is why promotion updates two columns in place instead.
- CI installs only `requirements-ci.txt`. Any module imported by a test must import without `azure` or `pyodbc`.
- Production writes only via the normal entrypoints. No script the monthly cron runs may change behaviour for months dated May 2026 or later.
- Verify with `bash ops/run_repo_checks.sh`. Baseline on an environment lacking `requests` is 10 pre-existing import errors; the count must not rise.

## Review Focus

1. **A month mixing old- and new-format reports.** Both spellings appear in the concatenated frame; each must land in its own column with no duplicate-named column. Covered in Task 2.
2. **A report carrying both spellings.** Never observed, but it would silently corrupt a row. Must warn and prefer the current spelling rather than merge. Covered in Task 2.
3. **Comma-formatted values in the new columns** (`"11,700"`). The `.str` accessor turns real numbers into NaN unless the series is cast to string first; new columns must get the same coercion as existing ones. Covered in Task 2.
4. **A post-May-2026 month after promotion.** The new columns must be `NULL`, not `0`, and `petrol_e20`/`ethanol_e100` must be byte-identical to pre-change output. Covered in Task 2.
5. **dbt incremental after the models gain columns.** `on_schema_change` is unset and dbt 1.8 defaults to `ignore`, so new columns would be silently absent from existing curated tables while every run reports success. Covered in Task 4.
6. **`inserted_at` after promotion.** Curated models copy `inserted_at` from raw, so any loss in raw propagates on the next full refresh and is then unrecoverable. Covered in Task 7.
7. **Rows present in the parallel table but not production.** A full reprocess regenerates the office-rename duplication (19 codes seen in the May 2026 pilot), so the parallel table legitimately holds rows production does not. An in-place update must ignore them rather than insert them. Covered in Task 7.

---

### Task 1: Schema migration adding the two columns to all six tables

**Files:**
- Create: `sql/migrations/2026-10-24_legacy_fuel_columns.sql`
- Test: `tests/test_legacy_fuel_migration_contract.py`

**Interfaces:**
- Produces: columns `ethanol`, `petrol_ethanol` (`NVARCHAR(MAX) NULL`) on `fact_ev_data_by_rto`, `fact_ev_data_by_state`, `fact_oem_data_by_state_and_category` and their three `staging_*` counterparts, at matching ordinal positions within each pair.

- [x] **Step 1: Write the failing test**

```python
TABLES = [
    "fact_ev_data_by_rto", "staging_fact_ev_data_by_rto",
    "fact_ev_data_by_state", "staging_fact_ev_data_by_state",
    "fact_oem_data_by_state_and_category",
    "staging_fact_oem_data_by_state_and_category",
]

def test_migration_adds_both_columns_to_every_table(self):
    sql = MIGRATION_PATH.read_text()
    for table in TABLES:
        self.assertIn(table, sql)
    self.assertIn("[ethanol]", sql)
    self.assertIn("[petrol_ethanol]", sql)

def test_migration_does_not_alias_into_current_columns(self):
    sql = MIGRATION_PATH.read_text()
    self.assertNotIn("ethanol_e100 = ", sql)
    self.assertNotIn("petrol_e20 = ", sql)

def test_migration_has_backup_and_rollback(self):
    sql = MIGRATION_PATH.read_text()
    self.assertIn("backup_20261024", sql)
    self.assertIn("XACT_ABORT", sql)
    self.assertIn("Rollback:", sql)

def test_migration_is_idempotent_and_does_not_rebuild_tables(self):
    sql = MIGRATION_PATH.read_text()
    self.assertIn("COL_LENGTH", sql)
    # A rebuild would rewrite 2.3M rows and reorder columns for no gain.
    self.assertNotIn("sp_rename", sql)
```

- [x] **Step 2: Run it to make sure it fails**

Run: `python3 -m unittest tests.test_legacy_fuel_migration_contract -v`
Expected: FAIL — migration file does not exist.

- [x] **Step 3: Write the migration**

Six `ALTER TABLE ... ADD [ethanol] NVARCHAR(MAX) NULL, [petrol_ethanol] NVARCHAR(MAX) NULL` statements, in the same column order every time, each guarded by `COL_LENGTH` so a re-run is a no-op. No table rebuild: `ALTER ... ADD` appends, so issuing it identically to both halves of a pair keeps their ordinals aligned, which is all the positional staging-to-final transfer needs. Avoiding a rebuild matters — the RTO table holds 2.3M rows and the alternative would rewrite all six tables to gain nothing but column cosmetics.

Back each table up to `<table>_backup_20261024` first and abort if the backup exists, mirroring `sql/migrations/2026-06-19_vahan_fuel_schema_refresh.sql`. Header comment states the measured loss figures and the rollback (`DROP COLUMN` on both, per table).

- [x] **Step 4: Run tests to verify they pass**

Run: `python3 -m unittest tests.test_legacy_fuel_migration_contract -v`
Expected: PASS (4 tests)

- [x] **Step 5: Add the module to the repo check runner**

Append `tests.test_legacy_fuel_migration_contract` to `UNIT_TEST_MODULES` in `ops/run_repo_checks.sh:10`.

- [x] **Step 6: Verify the suite**

Run: `bash ops/run_repo_checks.sh`
Expected: error count still 10, no new failures.

- [x] **Step 7: Commit**

```bash
git add sql/migrations/2026-10-24_legacy_fuel_columns.sql tests/test_legacy_fuel_migration_contract.py ops/run_repo_checks.sh
git commit -m "feat: migration adding legacy fuel columns to raw and staging tables"
```

---

### Task 2: Teach shared preprocessing the legacy headers

**Files:**
- Modify: `pipeline_constants.py:110-148` (add two entries to `COMMON_FUEL_COLUMN_RENAME_MAP`)
- Modify: `etl_preprocessing.py` (per-file header handling in `_read_report`)
- Test: `tests/test_schema_regression.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `COMMON_FUEL_COLUMN_RENAME_MAP` gains `"ETHANOL": "ethanol"` and `"PETROL/ETHANOL": "petrol_ethanol"`. `NUMERIC_OUTPUT_COLUMNS` at `etl_preprocessing.py:30` derives from that map, so both columns get comma-stripping and `Int64` coercion with no further change. Preprocessing output for all three pipelines gains both columns.

- [x] **Step 1: Write the failing tests**

```python
def test_mixed_format_month_produces_one_column_per_fuel(self):
    # one report with ETHANOL / PETROL/ETHANOL, one with ETHANOL(E100) / PETROL(E20)
    result = run_rto_preprocessing_over(old_format_report, new_format_report)
    self.assertEqual(list(result.columns).count("petrol_e20"), 1)
    self.assertEqual(list(result.columns).count("petrol_ethanol"), 1)

def test_legacy_and_current_columns_stay_separate(self):
    result = run_rto_preprocessing_over(old_format_report)
    self.assertEqual(result.loc[0, "petrol_ethanol"], 11700)
    self.assertTrue(pd.isna(result.loc[0, "petrol_e20"]))

def test_comma_formatted_legacy_values_are_coerced(self):
    # raw cell is the string "11,700"
    result = run_rto_preprocessing_over(old_format_report)
    self.assertEqual(result.loc[0, "petrol_ethanol"], 11700)

def test_new_format_month_leaves_legacy_columns_null(self):
    result = run_rto_preprocessing_over(new_format_report)
    self.assertEqual(result.loc[0, "petrol_e20"], 63595)
    self.assertTrue(pd.isna(result.loc[0, "petrol_ethanol"]))
    self.assertTrue(pd.isna(result.loc[0, "ethanol"]))

def test_report_with_both_spellings_warns_and_keeps_current(self):
    with self.assertLogs(level="WARNING") as captured:
        result = run_rto_preprocessing_over(report_with_both_spellings)
    self.assertIn("both", " ".join(captured.output))
    self.assertEqual(result.loc[0, "petrol_e20"], 63595)
```

- [x] **Step 2: Run them to make sure they fail**

Run: `python3 -m unittest tests.test_schema_regression -v`
Expected: FAIL — `petrol_ethanol` is not an output column.

- [x] **Step 3: Add the two map entries**

In `pipeline_constants.py`, insert `"ETHANOL": "ethanol"` and `"PETROL/ETHANOL": "petrol_ethanol"` keeping the map's existing alphabetical-by-source-key ordering. The resulting `.values()` order is the contract Task 3 asserts against `sources.yml`, so record it.

- [x] **Step 4: Handle a report carrying both spellings**

Add `_drop_duplicate_legacy_headers(df, report_path)` to `BaseExcelPreprocessor`, called from `_read_report` beside the existing `_normalize_report_total_column`. When a file contains a legacy header *and* its current counterpart, drop the legacy one and log a warning naming the file. Per-file placement is required: the rename runs after `pd.concat`, so a month mixing formats would otherwise produce two identically-named columns and break `_coerce_numeric_output_columns`.

- [x] **Step 5: Run tests to verify they pass**

Run: `python3 -m unittest tests.test_schema_regression -v`
Expected: PASS

- [x] **Step 6: Verify the suite**

Run: `bash ops/run_repo_checks.sh`
Expected: error count still 10.

- [x] **Step 7: Commit**

```bash
git add pipeline_constants.py etl_preprocessing.py tests/test_schema_regression.py
git commit -m "feat: load pre-May-2026 Vahan fuel headers into their own columns"
```

---

### Task 3: Update the source and curated dbt contracts

**Files:**
- Modify: `climate_dot_dbt/models/curated/sources.yml` (three source tables)
- Modify: `tests/test_dbt_contracts.py:84`
- Modify: `tests/test_schema_regression.py:140-146`
- Test: both files above

**Interfaces:**
- Consumes: the `.values()` ordering fixed in Task 2.
- Produces: `sources.yml` column lists matching `COMMON_FUEL_COLUMN_RENAME_MAP.values()` exactly, in order.

- [x] **Step 1: Write the failing tests**

`test_dbt_contracts.py` already derives `EXPECTED_SOURCE_COLUMNS` from the map, so Task 2 made `test_raw_sources_match_expected_columns` fail. Replace the blanket prohibition at line 84 and pin the intent instead:

```python
def test_curated_models_expose_legacy_fuel_columns_separately(self):
    for filename in MODEL_FILES:
        sql = (curated_dir / filename).read_text()
        # Legacy headers are their own columns, never aliased into the
        # current semantic fields. The 2026-06-21 repair reverted exactly
        # that aliasing, so both names must appear independently.
        self.assertIn("as petrol_ethanol", sql)
        self.assertIn("as petrol_e20", sql)
        self.assertIn("as ethanol", sql)
        self.assertIn("as ethanol_e100", sql)
        self.assertNotIn("electric_vehicles", sql)
```

`test_schema_regression.py:146` asserts strict equality between one sample workbook's headers and the map keys, which Task 2 also broke. Parameterise it over both formats:

```python
def test_sample_workbooks_cover_both_header_generations(self):
    for workbook, absent in [
        (NEW_FORMAT_WORKBOOK, {"ETHANOL", "PETROL/ETHANOL"}),
        (OLD_FORMAT_WORKBOOK, {"ETHANOL(E100)", "PETROL(E20)"}),
    ]:
        df = pd.read_excel(workbook, skiprows=3, index_col=0)
        headers = set(df.columns)
        self.assertTrue(headers <= set(COMMON_FUEL_COLUMN_RENAME_MAP.keys()) | {"Unnamed: 1"})
        self.assertEqual(headers & absent, set())
```

- [x] **Step 2: Run them to make sure they fail**

Run: `python3 -m unittest tests.test_dbt_contracts tests.test_schema_regression -v`
Expected: FAIL on source column mismatch and on the missing old-format workbook.

- [x] **Step 3: Add an old-format sample workbook**

Copy one pre-May-2026 report (27 columns, carrying `ETHANOL` and `PETROL/ETHANOL`) next to the existing sample and reference it as `OLD_FORMAT_WORKBOOK`. The repo has 265,493 such files on the VM under `rto_level/rto_level_ev_data/<state>/<office>/<year>/<month>/reportTable.xlsx`.

- [x] **Step 4: Add both columns to each source table in `sources.yml`**

Three tables, two columns each, in `COMMON_FUEL_COLUMN_RENAME_MAP.values()` order. Descriptions: "Number of vehicles reported under the pre-May-2026 ETHANOL header" and "...PETROL/ETHANOL header".

- [x] **Step 5: Run tests to verify they pass**

Run: `python3 -m unittest tests.test_dbt_contracts tests.test_schema_regression -v`
Expected: PASS

- [x] **Step 6: Commit**

```bash
git add climate_dot_dbt/models/curated/sources.yml tests/test_dbt_contracts.py tests/test_schema_regression.py tests/fixtures/
git commit -m "feat: contract both Vahan fuel header generations"
```

---

### Task 4: Surface the columns in the three curated models

**Files:**
- Modify: `climate_dot_dbt/models/curated/rto_wise_ev_data.sql`
- Modify: `climate_dot_dbt/models/curated/oem_wise_ev_data.sql`
- Modify: `climate_dot_dbt/models/curated/state_wise_ev_data.sql`
- Test: `tests/test_dbt_contracts.py`

**Interfaces:**
- Consumes: source columns from Task 3.
- Produces: curated columns `ethanol`, `petrol_ethanol` as `INT`.

- [x] **Step 1: Write the failing test**

```python
def test_models_declare_append_new_columns(self):
    for filename in MODEL_FILES:
        sql = (curated_dir / filename).read_text()
        # Default on_schema_change is 'ignore' in dbt 1.8, which would add
        # the SELECT columns while silently leaving them off the existing
        # table and still reporting success.
        self.assertIn("on_schema_change='append_new_columns'", sql)
```

- [x] **Step 2: Run it to make sure it fails**

Run: `python3 -m unittest tests.test_dbt_contracts -v`
Expected: FAIL — `on_schema_change` absent.

- [x] **Step 3: Add the columns and the config to all three models**

One `CAST(REPLACE(<col>, ',', '') AS INT) as <col>` line per new column, placed beside its counterpart (`ethanol` next to `ethanol_e100`, e.g. `rto_wise_ev_data.sql:32`). Add `on_schema_change='append_new_columns'` to each model's `config()` block.

- [x] **Step 4: Run tests to verify they pass**

Run: `python3 -m unittest tests.test_dbt_contracts -v`
Expected: PASS

- [x] **Step 5: Parse the project against a real profile**

Run: `RUN_DBT_PARSE=1 DBT_PROFILES_DIR=/home/climate_dot_data/.dbt bash ops/run_repo_checks.sh`
Expected: dbt parse succeeds, error count still 10.

- [x] **Step 6: Commit**

```bash
git add climate_dot_dbt/models/curated/
git commit -m "feat: expose legacy fuel columns in curated models"
```

---

### Task 5: Reprocess OEM and State into parallel tables

**Files:**
- Create: `oem_level/fuel_mapping_fix/reprocess_to_fuelfix.py`
- Create: `state_level/fuel_mapping_fix/reprocess_to_fuelfix.py`
- Create: `sql/migrations/2026-10-24_oem_state_fuelfix_parallel_tables.sql`

**Interfaces:**
- Consumes: the map from Task 2, so no preprocessing subclass is needed — the production preprocessors now emit both columns.
- Produces: `fact_oem_data_by_state_and_category_fuelfix` and `fact_ev_data_by_state_fuelfix`, populated 2013 → latest month with raw files on disk.

- [x] **Step 1: Create the parallel tables**

Generate DDL from the production tables so types cannot drift, mirroring `sql/migrations/2026-10-23_rto_fuelfix_parallel_tables.sql`. Both new columns before `inserted_at`; fact and staging column order must match.

- [x] **Step 2: Write the drivers**

Mirror `rto_level/fuel_mapping_fix/reprocess_to_fuelfix.py`: resumable progress file, per-month preprocess then ingest, its own CSV prefix, and **never** invoke `upload_files_to_blob_storage.py` — that step deletes the raw files the reprocess reads. After Task 2 these can call the production preprocessing scripts directly.

- [x] **Step 3: Run one month and verify the gap closes**

Run each driver for a single 2025 month.
Expected: `SUM(total) - SUM(all fuel columns) = 0`, and `petrol_e20`/`ethanol_e100` unchanged from production for that month.

- [x] **Step 4: Run the full range**

Expected: no `FAILED` or `Traceback` in either log; every month present on disk recorded in the progress file.

- [x] **Step 5: Commit**

```bash
git add oem_level/fuel_mapping_fix/ state_level/fuel_mapping_fix/ sql/migrations/2026-10-24_oem_state_fuelfix_parallel_tables.sql
git commit -m "feat: parallel fuel-mapping reprocess for OEM and State"
```

---

### Task 6: Verify the parallel tables (dedup no longer on the critical path)

**Files:**
- Create: `sql/migrations/2026-10-24_fuelfix_verification.sql`

**Interfaces:**
- Produces: parallel tables verified against production, with the office-rename duplicates identified but left in place.

**Status: done for RTO on 2026-10-23.** The reprocess produced 122 extra rows across 18 renamed `rto_code`s, all in May 2026 (`DL3 DL4 DL6-DL13 DL52 DL53 MH16 MH58 MP16 MZ9 PY51 TN631`), each the same office under its newer name from a second folder on disk. They were backed up to `fact_ev_data_by_rto_fuelfix_renamedup_backup_20261023` and deleted **from the fuelfix table only**. After removal, row counts and EV counts match production exactly in all 14 years. Repeat this check for OEM and State.

Because Task 7 updates two columns on existing production rows and inserts nothing, the duplicates would never have reached production anyway; removing them was to make the validation unambiguous.

- [x] **Step 1: Quantify the duplication**

A full reprocess regenerates the known office-rename duplication, because the disk holds two folders per renamed code (e.g. `KEYI PANYOR_AR27` and `Keyi Panyor_AR27`). 19 codes appeared in the May 2026 pilot. Report affected `(rto_code, rto_name, year, month)` rows and confirm every one is a known renamed code; an unexpected code means the reprocess picked up a folder it should not have.

- [x] **Step 2: Confirm the join key is safe for an in-place update**

Expected: zero duplicate `(date, state, rto_code, rto_name, vehicle_class)` groups in the parallel table. Production currently has 3 such groups out of 2,324,451 rows; list them and confirm each is one of the known renamed codes, since an ambiguous key makes `UPDATE ... FROM` pick a row arbitrarily.

- [x] **Step 3: Confirm the missing months need nothing**

2026-08 and 2026-09 have no raw files on disk, so the parallel table stops earlier than production. Both post-date the rename, so their legacy columns are legitimately `NULL` and the in-place update leaves them untouched. No blob download is required; verify that both months' unattributed share in production is already 0.00%.

- [x] **Step 4: Reconcile against production**

Expected: for every month, parallel `total` equals production `total`; `electric_bov`, `pure_ev` and every already-working fuel column are unchanged; `SUM(total) - SUM(all fuel columns) = 0` in every month; years 2013–2022 are identical to production apart from the ~317 vehicles nationally that the legacy columns held.

- [x] **Step 5: Commit**

```bash
git add sql/migrations/2026-10-24_fuelfix_verification.sql
git commit -m "test: verify parallel fuel-mapping tables against production"
```

---

### Task 7: Fill the two columns in production and refresh the curated layer

**Files:**
- Create: `sql/migrations/2026-10-24_backfill_legacy_fuel_columns.sql`
- Create: `docs/runbooks/fuel_header_mapping_cutover.md`

**Interfaces:**
- Consumes: verified parallel tables from Task 6; schema from Task 1; models from Task 4.
- Produces: production raw tables with `ethanol`/`petrol_ethanol` populated for pre-May-2026 months, every other column including `inserted_at` unchanged.

- [x] **Step 1: Capture the pre-change fingerprint**

Record, per table **and per month**: row count, `SUM(total)`, `SUM(electric_bov + pure_ev)`, and the set of distinct `inserted_at` values with their row counts. These are the invariants Step 5 checks. Production currently holds 5 distinct `inserted_at` days.

The EV invariant is the hard gate: **`SUM(electric_bov + pure_ev)` must be bit-identical per month before and after.** The fix adds two columns and touches nothing else, so any EV movement at all means the update hit a column it should not have. Verified +0.0000% across the first 94 reprocessed months; it must hold for all of them.

- [x] **Step 2: Write the backfill**

```sql
UPDATE p
SET p.ethanol        = f.ethanol,
    p.petrol_ethanol = f.petrol_ethanol
FROM dbo.fact_ev_data_by_rto AS p
INNER JOIN dbo.fact_ev_data_by_rto_fuelfix AS f
  ON  f.[date]        = p.[date]
  AND f.state         = p.state
  AND f.rto_code      = p.rto_code
  AND f.rto_name      = p.rto_name
  AND f.vehicle_class = p.vehicle_class;
```

Three points this SQL does not carry. Back each table up to `<table>_prefuelfix_backup_20261024` first and abort if the backup already exists, per the repo's migration pattern. Set **only** these two columns — never `inserted_at`, never `ethanol_e100`, never `petrol_e20`. Use `INNER JOIN` so rows present only in the parallel table (the office-rename duplicates) are ignored rather than inserted. Repeat for the OEM and State pairs. Header records the rollback: `UPDATE <table> SET ethanol = NULL, petrol_ethanol = NULL`.

- [x] **Step 3: Run it**

Run the migration against production.
Expected: rows affected equals the parallel table's row count minus its known office-rename duplicate rows.

- [x] **Step 4: Verify the two columns landed**

Expected: for every month before May 2026, `SUM(total) - SUM(all fuel columns) = 0`; for May 2026 onward, `ethanol` and `petrol_ethanol` are `NULL` on every row.

- [x] **Step 5: Verify nothing else moved**

Expected: row count, `SUM(total)`, `SUM(electric_bov + pure_ev)`, and the distinct `inserted_at` values with their row counts are all **identical** to the Step 1 fingerprint. Any change to `inserted_at` means the update touched a column it should not have — roll back and fix before continuing, because the next curated refresh would make the loss permanent.

- [x] **Step 6: Full-refresh the curated models**

Run: `dbt run --full-refresh --select rto_wise_ev_data oem_wise_ev_data state_wise_ev_data`

A full refresh is required even with `on_schema_change='append_new_columns'`, because that setting adds the column without backfilling existing rows. Curated `inserted_at` is sourced from raw, so Step 5 passing is what makes this refresh safe.

- [x] **Step 7: Verify the curated layer**

Expected: curated row count equals raw row count per table; the two new columns are `INT`; EV totals per year unchanged from Step 1; curated `inserted_at` distinct values match raw's.

- [x] **Step 8: Run one monthly pipeline end to end**

Run `bash rto_ev_data_etl.sh` for the most recent month.
Expected: completes; because that month post-dates the rename, the two new columns are `NULL` while `petrol_e20` and `ethanol_e100` are unchanged.

- [x] **Step 9: Retire the one-off subclass**

`rto_level/fuel_mapping_fix/rto_preprocessing_fuelfix.py` exists only because production preprocessing could not emit these columns. Task 2 removed that need. Delete it and point the driver at the production preprocessing script, so there is one code path.

- [x] **Step 10: Write the runbook**

Record what changed, the backup table names, the rollback statement, that `ethanol`/`petrol_ethanol` are deliberately *not* merged with `ethanol_e100`/`petrol_e20`, and that the parallel tables can be dropped once the curated layer is verified.

- [x] **Step 11: Commit**

```bash
git add sql/migrations/2026-10-24_backfill_legacy_fuel_columns.sql docs/runbooks/fuel_header_mapping_cutover.md
git rm rto_level/fuel_mapping_fix/rto_preprocessing_fuelfix.py
git commit -m "feat: backfill legacy fuel columns in production"
```

---

### Task 8: Update the query agent context

**Files:**
- Modify: `docs/llm_query_agent_context.md`

- [x] **Step 1: Remove the obsolete warnings**

The section "`total` is larger than the 36 fuel columns add up to", the state-variance table, the cross-state fuel-comparison ban in the limitations block, and the `petrol` worked example all describe a defect that no longer exists.

- [x] **Step 2: Document the two new columns**

Add `ethanol` and `petrol_ethanol` to the measures list with one line each: they carry the pre-May-2026 header generation, are `NULL` for later months, and must **not** be added to `ethanol_e100`/`petrol_e20` unless the user asks for a combined fuel figure — in which case say the two are being combined.

- [x] **Step 3: Verify the sample queries**

Re-run the Q10 fuel-mix query.
Expected: `other_or_unattributed` is near zero in every year, so its caveat can be softened to cover genuine other fuels only.

- [x] **Step 4: Commit**

```bash
git add docs/llm_query_agent_context.md
git commit -m "docs: fuel split is now complete across all periods"
```
