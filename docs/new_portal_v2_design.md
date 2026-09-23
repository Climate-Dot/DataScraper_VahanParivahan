# New Portal (Parivahan Analytics) V2 — RTO Grain Design

Status: Phase 0 (access & verification) complete 2026-08-12. Phase 1 (schema/grain decisions) complete 2026-08-13, including both crosswalks built and committed as real seed files, and a first `new_portal/` Python module with tests. Scope is RTO grain only — OEM and State are deferred until RTO proves the pattern in production. Phase 2 (fetch/ingest/migration/entrypoint) built 2026-08-18, unit-tested and smoke-tested live against two RTOs, but not yet run at scale or against the real database. Decision 4 was materially corrected on 2026-08-18 (the RTO ID relationship is deterministic, and the live RTO count is 1,676, not 1,409) — read it before sizing Phase 2.

Background and the full investigation trail (endpoint discovery, reconciliation check, etc.) live in the evaluation artifact referenced in project memory; this doc is the durable, git-tracked record of what was actually decided, per the "artifacts are snapshots, decisions belong in the repo" call made 2026-08-13.

## Source

`https://analytics.parivahan.gov.in/analytics/` — a session-cookie-based JSON API (load `/analytics/publicdashboard/vahan?lang=en` once, reuse the cookie jar). No login/API key. Confirmed reachable from the production VM as of 2026-08-12.

Key endpoints (see artifact for full list and confirmed shapes):
- `json_rtos?stateCode=` — RTO dimension lookup (new numeric `rtoCode` + `rtoName` per state)
- `durationWiseRegistrationTable` — one row per calendar month, filtered by fuel/category/archive-status
- `fueltypedonutchart` — full per-fuel breakdown in one call, scopable to one vehicle class via `vehicleClasses=` (exact label match)
- `classdistribution` — full per-class breakdown in one call
- `statusdistribution` — full per-archive-status breakdown in one call
- `vahan/registration/dashboardcount` — the dashboard's own headline total-count tile. Same filter params as the others, but **year-level only, no month breakdown** (so it can't replace `durationWiseRegistrationTable` for actual ingestion) — its value is as an independent reconciliation check, see below. Response shape is `{"totalTransactions": "2,18,788"}` — Indian comma-grouped string, needs the same strip-commas-then-cast pattern V1's dbt models already use.

## Decisions

### 1. New tables, existing tables untouched
V2 writes to new tables. **Name confirmed by Monish 2026-08-18: `fact_ev_data_by_rto_v2`** (staging `stg_ev_data_by_rto_v2`). The existing `fact_ev_data_by_rto` / `rto_wise_ev_data` and their backup tables stay exactly as they are — no schema changes, no shared writes. This follows directly from the August 2026 historical remediation's own conclusion: those tables and their backups are irreplaceable once Vahan access closes 2026-08-15, and mixing a differently-grained source into them risks the same kind of silent corruption that remediation spent a week fixing. **Confirmed with Monish 2026-08-18: new tables, existing ones untouched.**

### 2. Schema shape — flat/OBT, matching V1 (decided 2026-08-13)
Considered a light star schema (fact table + small `dim_rto`/`dim_vehicle_class` tables, one join hop) as an improvement over V1's fully denormalized wide table — the case for it: `vehicle_class` is only 76 fixed values and `rto` is ~1,000, both cheap to normalize with no update-anomaly risk. Considered and rejected snowflaking further (splitting `vehicle_class` into separate category/use_type sub-tables) since that mapping is fixed and 1:1 — extra joins for no benefit. **Decision: stay flat, matching V1's pattern** — dimension values (`state`, `rto_name`, `vehicle_class`, `vehicle_type`, `vehicle_category`, `vehicle_use_type`) are stored directly as columns on every fact row, not as foreign keys to separate tables. Simpler to build and query, consistent with the existing dbt models, and given the low expected read concurrency (a handful of backend API consumers, not a high-QPS service), join elimination wasn't worth the added modeling complexity. The RTO crosswalk and vehicle-class crosswalk (decisions 3–4) are still built as real reference tables — they're just used to *populate and validate* the flat columns during preprocessing, not joined at query time, the same way `rto_code_to_district_mapping` is used in V1 today.

### 3. Grain / unique key
Proposed grain for the new raw table:

- `date` (year + month only — parsed from `yearAsString`, e.g. `"2026-August"`; the source has no day-level granularity, unlike today's table which carries a `date` column populated from a monthly report's nominal date)
- `state_code`
- `rto_code` (new numeric ID from `json_rtos`, e.g. Pune = 12) — see decision 4
- `vehicle_class` (from `classdistribution`'s vocabulary, ~58–76 values, near-1:1 with today's DB values)
- `status_scope` (`ACTIVE` | `ALL_STATUSES`) — new dimension, added for decision 6

`vehicle_type`, `vehicle_category`, `vehicle_use_type` are **not** independently queried — they're populated from the static crosswalk (decision 5) keyed on `vehicle_class`, and stored as flat columns per decision 2.

### 4. RTO identity — adopt the new numeric ID as canonical
V2's canonical RTO key is `(state_code, rto_code_new)`, using the portal's small per-state integer `rtoCode` from `json_rtos`.

**Correction, 2026-08-18 — the two ID schemes are not unrelated after all.** This decision originally stated that the new numeric `rtoCode` was "unrelated to today's alphanumeric `rto_code`" and that Pune being old `MH12` / new `12` was "a one-off match, not a pattern". That was wrong, and it led the first crosswalk builder to match on normalized office name. Verified against all 36 states and all 1,676 live RTOs:

```
legacy_rto_code == legacy_state_prefix + str(new_rto_code)
```

with **zero exceptions**, and the portal's own `rtoName` now literally ends in `" - <STATE_CODE><rtoCode>"` (e.g. `"PUNE - MH12"`, `"BALASORE RTO - OR1"`) for **1,676 of 1,676** RTOs. The only prefix alias is Odisha: the portal's state code is `OR` while today's `rto_code`s use the `OD` prefix (`OD1` ↔ `OR`/`1`).

`new_portal/build_rto_crosswalk.py` was rewritten accordingly. It now *derives* the mapping arithmetically and *verifies* each row against the code the portal embeds in `rtoName`, warning loudly on any disagreement, instead of fuzzy-matching names. This matters beyond tidiness: the portal changed `rtoName`'s format between 2026-08-13 and 2026-08-18 (adding the code suffix), which would have degraded every single one of the old builder's 1,395 exact matches to fuzzy matches on the next run. Name matching was the fragile part; the code relationship is the stable one.

It also resolved 3 of the 13 previously-unmatched RTOs (`DL51` → RAJPUR ROAD/VIU BURARI, `HP3` → RLA SHIMLA HP-03/HP-07(URBAN), `TG3` → RTA WARANGAL URBAN — all pure name mismatches), and changed **zero** of the 1,396 mappings the old name-matching builder had already found — an independent confirmation that both methods agree where they overlap.

**The crosswalk is now driven by the live portal list, not V1's history.** V2 ingests forward from cutover (decision 8), so it must cover offices that never appeared in V1. [`climate_dot_dbt/seeds/rto_code_crosswalk.csv`](/Users/monish/DataScraper_VahanParivahan/climate_dot_dbt/seeds/rto_code_crosswalk.csv) is now **1,686 rows** with a `link_status` column:

| `link_status` | Count | Meaning |
| --- | --- | --- |
| `linked` | 1,399 | On the portal and in V1 history. The normal case. |
| `portal_only` | 277 | On the portal, no V1 history. **V2 must still ingest these.** |
| `legacy_only` | 10 | In V1 history, absent from the portal. V2 cannot ingest these. |

1,399 + 277 = 1,676 live portal RTOs; 1,399 + 10 = 1,409 legacy RTOs. Both sides fully accounted for, no duplicates on either key.

The 277 `portal_only` offices are concentrated in Rajasthan (84), Haryana (80), Himachal Pradesh (33), Odisha (39) and Andaman & Nicobar (13) — consistent with genuine office expansion plus sub-offices the old dashboard never listed separately. **This raises Phase 2's call volume by ~19% over the 1,409 figure the estimate below was based on.**

The 10 `legacy_only` offices are all small or recently created: `AP837` Devarapalli, `AS36` BAJALI, `GJ40` THARAD, `HP128` RLA SUNNI, `HP129` RTO Dehra, `MN13` Kakching, `MN14` JIRIBAM, `MN15` NONEY, `MZ10` KHAWZAWL, `MZ11` HNAHTHIAL. Their V1 history stays intact in `fact_ev_data_by_rto`; they simply have no forward path on the new portal. Worth one look at whether the portal folds them into a parent office (which would double-count them into that office's numbers) — but with 10 small offices, this is a footnote, not a blocker.

Building the original crosswalk also surfaced a real bug, still fixed and still relevant: `fact_ev_data_by_rto.state` uses different spelling than `pipeline_constants.STATE_LIST` for two union territories ("Andaman and Nicobar", "Dadara and Nagar Havelli") — both are aliased in `STATE_NAME_TO_NEW_PORTAL_CODE`.

### 5. vehicle_type / vehicle_category / vehicle_use_type via static crosswalk
**Correction, 2026-08-14 (Monish):** an earlier version of this decision described these as "raw Vahan columns" with no new-portal equivalent. That was wrong. **`vehicle_class` was always the only genuine raw column, on the old portal same as the new one.** `vehicle_type`, `vehicle_category`, and `vehicle_use_type` are a business-defined categorization that V1's own preprocessing already applies via a join, not something Vahan (old or new) ever provided directly — confirmed at the code level in `rto_level_data_pre_processing.py`'s `apply_mapping()`: `pd.merge(df, mapping_df, left_on="Unnamed: 1", right_on="Vehicle Class", how="left")` against **[`Table and Mapping V2.xlsx`](/Users/monish/DataScraper_VahanParivahan/Table%20and%20Mapping%20V2.xlsx)**'s `Mapping` sheet, with unmatched classes defaulting to `"Others"` (`etl_preprocessing.DEFAULT_DIMENSION_VALUE`). This means there was never a real gap here on the source side — only in how this decision was first described.

This is genuinely good news for V2: since the new portal supplies `vehicle_class` cleanly (near-1:1 with today's DB values, `classdistribution` endpoint), the *exact same mapping file V1 already depends on* can be reused unchanged, rather than needing a new crosswalk reverse-engineered from historical data. `new_portal/build_vehicle_class_crosswalk.py` was rewritten accordingly: it now reads `Table and Mapping V2.xlsx` directly as the authoritative source (matching V1's own join), and only defaults to `"Others"` for any DB-observed class missing from the file — exactly mirroring V1's own fallback behavior, rather than inventing a different one.

Result, regenerated 2026-08-14: of 76 classes with observed production data, 73 matched the mapping file exactly and 3 (`SCHOOL BUS`, `VINTAGE MOTOR VEHICLE`, `PULLER TRACTOR`) aren't in the file and correctly default to `Others`/`Others`/`Others`. (The file also has a couple of unused near-duplicate rows — e.g. both `ROAD ROLLER` and `ROAD ROLLERS` are listed, both mapping to `Others` anyway — harmless, not worth cleaning up.) Output is **[`climate_dot_dbt/seeds/vehicle_class_crosswalk.csv`](/Users/monish/DataScraper_VahanParivahan/climate_dot_dbt/seeds/vehicle_class_crosswalk.csv)**, now with a `source` column (`authoritative_mapping_file` vs `defaulted_others`) so it's visible which rows came from the real mapping vs. a fallback.

**Consequence for Phase 2 ingestion code, not just the crosswalk builder:** it should implement the same "class not in the mapping file → default to Others, log a warning" rule at runtime, not just at crosswalk-build time — the new portal could report a `vehicle_class` that's never appeared in historical data (and so isn't in this seed file) at some point, and it should degrade the same way V1 already does, not fail.

### 6. Total definition — `ALL_STATUSES` only (decided 2026-08-13, revised 2026-08-19)
**Current decision (2026-08-19, Monish): ingest `ALL_STATUSES` only.** The original 2026-08-13 decision was to compute every fuel/class row twice, under both scopes; that was reversed after measuring what `ACTIVE` actually means (see the caveat below). `status_scope` remains a real column and part of the grain, so re-enabling `ACTIVE` later is a `--status-scopes` flag plus a re-run, not a schema migration.

The two scopes are:
- `ACTIVE`: `archiveTypeAC` + `archiveTypeANC` only (the portal's own default view)
- `ALL_STATUSES`: all 5 `archiveType*` params passed explicitly (matches today's historical `total` semantics — validated in the Phase 0 reconciliation check, ~0.5% drift explained by ordinary data lag, not a definitional gap)

**Resolved 2026-08-13:** confirmed there's no single-call shortcut — read the actual `fetch()` param-building code in `fueltypedonutchart.js`/`classdistribution.js` directly (not guessed) and there's no `groupBy`/`breakdown`/status-pivot param on either endpoint. A separate `vehicleStatus` param exists but is a different, more granular, UI-hidden dimension (`ACTIVE`, `CONFISCATED AUCTION VEHICLE`, `NOC ISSUED`, ...), not a shortcut for this. **The doubling is real and unavoidable as designed** — budgeted into the call-volume estimate below.

**Important caveat found 2026-08-18 — `ACTIVE` is an as-of measure, not a fact about the month.** Comparing the two scopes via `dashboardcount` across three RTOs and three years:

| Year | MH/12 Pune | MH/44 Ambejogai | DL/1 |
| --- | --- | --- | --- |
| 2026 | +0.00% | +0.00% | +0.10% |
| 2020 | +11.16% | +18.51% | +5.07% |
| 2015 | +60.71% | +69.83% | +32.87% |

(percentage by which `ALL_STATUSES` exceeds `ACTIVE`.)

The gap is a function of **how long ago the registration happened**, because vehicles get archived/deregistered over time. Two consequences:

1. **`ACTIVE` for a given month is not stable.** It means "registered in that month *and still active as of the day we asked*", so the same historical month re-pulled a year later will return a smaller number. Storing it in a fact table records a snapshot whose meaning depends on the ingestion date, not on the month. `ALL_STATUSES` is the stable measure and the one that corresponds to V1's `total`.
2. **For freshly-ingested current-year months the two scopes are within ~0.1% of each other**, so the 2× call cost currently buys almost nothing — the divergence only builds up years later, and only for the `ACTIVE` series, which is precisely the series that will have gone stale by then unless old years get re-pulled.

**Resolved 2026-08-19 (Monish): drop `ACTIVE`, ingest `ALL_STATUSES` only.** It was costing double the API calls to store a number within ~0.1% of the one beside it, which would then go stale in place — and keeping it accurate for past months would mean periodically re-pulling history, which decision 8 (forward-only) explicitly does not do.

A finer-grained measurement the same day, per month within a single year for Pune, sharpened this. Within a recent year the decay is essentially flat (2024: 96.6%–97.8% still active across all twelve months), so the artifact worth worrying about — early months of a year reading lower than late months purely because they had longer to decay — is negligible. But 2019 showed a **cliff**, not a gradient: Jan–Jul sit at ~50% still-active while Aug–Dec sit at ~81–84%. Whatever administrative event caused that, it confirms the broader point — `ACTIVE` moves for reasons unrelated to how many vehicles were registered that month.

Separately: `statusdistribution` returned an empty payload (`{}`) for every RTO/year tried on 2026-08-18. It is not used by the pipeline, but the client method for it should be treated as unverified.

### 9. Second reconciliation check — the dashboard's own total tile (2026-08-13)
The Phase 0 reconciliation check (artifact, section F) compared the DB against `durationWiseRegistrationTable`'s month-by-month sum. Found and tested a third, independent source: `vahan/registration/dashboardcount`, the endpoint behind the dashboard's own headline count tile. Same Pune/2026 slice, same filters:

| Scope | `durationWiseRegistrationTable` (summed) | `dashboardcount` (direct) | Gap |
|---|---|---|---|
| EV fuels, 2026 | 30,232 | 30,337 | +0.35% |
| All fuels, 2026 | 217,566 | 218,788 | +0.56% |

Same signature as the first reconciliation check — small, same direction, consistent with ordinary data lag (a full day passed between the two pulls here, and the gap is proportionally larger than the same-day check, which tracks). This is a genuinely independent cross-check, not just re-deriving the same query twice: `dashboardcount` is the number the dashboard itself displays as authoritative, computed server-side, not assembled from `durationWiseRegistrationTable` rows client-side.

Both test calls used **state + RTO together** (`stateCode=MH&rtoCode=12`, i.e. Pune specifically, not a Maharashtra-wide aggregate) — `get_dashboard_count()` requires both as parameters, so it can't be called any other way. This matters for how it gets used in Phase 3: **the ongoing validation must run this check at the same per-RTO grain Phase 2 ingests at, not as a single coarser state- or nationwide-level rollup.** A state-level check could pass even with real per-RTO errors, if one RTO's overcount happens to cancel another's undercount in the sum — checking every RTO individually (or at minimum a meaningful per-RTO sample each month, not just one aggregate number) is what actually catches that. Concretely: after each monthly ingestion run, for every RTO ingested, call `get_dashboard_count()` scoped to that exact `(state_code, rto_code, year)` and compare against the ingested total for that RTO, flagging anything over ~1-2% drift — not just one nationwide or state-level number.

### 7. Fuel columns — full ~34-column taxonomy parity (decided 2026-08-13)
Keep all of today's fuel columns, not just the EV-relevant subset. In practice this is close to free: `fueltypedonutchart` already returns every non-zero fuel type in one call regardless of scope, so tracking 34 columns instead of 4 doesn't add API calls, just more columns per row. `bio_diesel_b100` and `bio_methane` have no confirmed new-portal equivalent (see artifact) — they'll be `NULL` going forward per this repo's existing null-over-fabricated-value rule, not dropped from the schema.

### 8. Backfill — forward from cutover only (decided 2026-08-13)
No backfill into 1990–2012 despite the new portal exposing it. The existing 2013–2026 dataset in the old tables stays frozen as the permanent historical record for that period; V2 starts from whichever month it goes live.

## Call volume and runtime (measured on the VM, 2026-08-19)

**Nationwide runtime is roughly 45 minutes, not the 8-16 hours first projected.** The fix was concurrency, but not the obvious kind.

### The finding: the portal serializes per session

Parallelising across RTOs while sharing one `requests.Session` gains almost nothing. Measured on a real 48-RTO fetch:

| Workers (one shared session) | Wall clock |
| --- | --- |
| 1 | 564s |
| 8 | 463s |
| 16 | 459s |

Flat past 8 — and 8 vs 16 being identical is the tell: that is a hard serialization point, not gradual saturation. The portal is a Java application (`JSESSIONID`), and servlet containers commonly serialize concurrent requests belonging to the same HTTP session. **The session was the bottleneck, not the server.**

Giving each worker thread its own session, same workload, same 8 threads: **32.4s vs 2.7s, an 11.9x speedup.** Throughput then scales with *session count*, at roughly 4 calls/sec per session — 2 sessions → 8.8/s, 4 → 17.5/s, 8 → 25.8/s.

### End-to-end result

Real fetch, 48 RTOs, one session per worker, 0.15s per-call sleep:

| Workers / sessions | Wall clock | Speedup |
| --- | --- | --- |
| 1 | 527s | 1.0x |
| 8 | 76s | **6.9x** |

Extrapolated to all 1,676 RTOs: **~45 minutes**. Output was **byte-identical** at 1, 4, 8 and 16 workers — rows are sorted back into target order before writing, so the CSV does not depend on how many threads produced it.

### On throttling

HTTP 429s do occur, but they are background noise from the portal rather than something concurrency provokes — the *single-worker* run saw more of them (14) than the 8-worker run (6), out of roughly 2,600 calls each. In both runs **zero requests exhausted their retries**, and both produced identical output.

One configuration did visibly provoke throttling: 8 sessions with **no** sleep at all. Retries papered over it, but that is not a design to ship, which is why `DEFAULT_SLEEP_SECONDS` stays at 0.2. A sweep across 2/4/8 sessions at 0.15-0.3s sleep produced zero 429s throughout.

### Call volume

Roughly 55-80 API calls per RTO per year (it varies severalfold with office size), so **~90,000-135,000 calls** nationwide — below the ~260,000 worst case the earlier per-month plan assumed.

### Caveat

The 45-minute figure extrapolates from 48 RTOs, and per-RTO cost varies severalfold. It also says nothing about whether eight sessions stay healthy for the full run. One real full-scale run is still needed before this goes on cron.

## Implementation so far (Phase 1, 2026-08-13)

- **[`new_portal/client.py`](/Users/monish/DataScraper_VahanParivahan/new_portal/client.py)** — session-based API client (`NewPortalClient`), covers all 5 endpoints used above, with retry/backoff. Tested in `tests/test_new_portal_client.py`.
- **[`new_portal/build_vehicle_class_crosswalk.py`](/Users/monish/DataScraper_VahanParivahan/new_portal/build_vehicle_class_crosswalk.py)** — regenerates the decision-5 crosswalk from `Table and Mapping V2.xlsx` (the authoritative source V1 already depends on), cross-checked for coverage against observed DB classes. Tested in `tests/test_vehicle_class_crosswalk_builder.py`.
- **[`new_portal/build_rto_crosswalk.py`](/Users/monish/DataScraper_VahanParivahan/new_portal/build_rto_crosswalk.py)** — regenerates the decision-4 crosswalk. Rewritten 2026-08-18: derives the legacy↔new code mapping arithmetically and verifies it against the code the portal embeds in `rtoName`, instead of the original fuzzy name matching. Driven by the live portal list, with the DB used only to attach legacy names and flag `legacy_only` rows (`--no-db` skips the DB entirely). Tested in `tests/test_rto_crosswalk_builder.py`, which now also asserts invariants on the committed seed itself.
- `pipeline_constants.STATE_NAME_TO_NEW_PORTAL_CODE` — the state name↔code mapping both builders depend on.
- All three new test modules are registered in `ops/run_repo_checks.sh`; `requests` added to `requirements-ci.txt` so they actually run in CI (previously only in `requirements.txt`, untested there).
- `selenium` also added to `requirements-ci.txt` (2026-08-18). Unrelated to V2, but CI was silently red on `main`: `utils.py` imports selenium at module scope and `etl_preprocessing.py` imports `utils`, so `tests/test_shared_etl_support.py` could not load under the CI dependency set. Confirmed pre-existing by running the checks against a clean `main`. Selenium is a plain pip install (unlike azure/pyodbc, which need system libs and use the guarded-import pattern), so pinning it is simpler than guarding the import.
- Nothing here is committed to git yet — sitting in the working tree pending review.

## Implementation (Phase 2, 2026-08-18)

Built, unit-tested, and **proven end to end against the production database on 2026-08-19** (see "First production run" below). Not yet run at full scale.

- **[`new_portal/schema.py`](/Users/monish/DataScraper_VahanParivahan/new_portal/schema.py)** — the single source of truth for the V2 column vocabulary: the portal's 34 fuel labels, the label→column slug rule, the grain key, and the exact CSV/table column order. The migration SQL and this module are asserted equal by a test, because ingestion does `INSERT INTO final SELECT * FROM staging` and a column-order drift would silently write values into the wrong columns.
- **[`new_portal/rto_fetch.py`](/Users/monish/DataScraper_VahanParivahan/new_portal/rto_fetch.py)** — replaces V1's scrape + missing-file-recovery + preprocessing in one step (no file download means nothing to recover and nothing to parse). Writes `new_portal_rto_ev_data_<year>.csv`.
- **[`new_portal/rto_ingest.py`](/Users/monish/DataScraper_VahanParivahan/new_portal/rto_ingest.py)** — reuses `BaseSqlServerIngestor` unchanged; only table names and replacement scope differ.
- **[`sql/migrations/2026-08-18_new_portal_rto_v2_tables.sql`](/Users/monish/DataScraper_VahanParivahan/sql/migrations/2026-08-18_new_portal_rto_v2_tables.sql)** — creates `fact_ev_data_by_rto_v2` + `staging_fact_ev_data_by_rto_v2` and two indexes. Creates only; touches nothing existing, so the rollback is just dropping the two tables.
- **[`new_portal_rto_etl.sh`](/Users/monish/DataScraper_VahanParivahan/new_portal_rto_etl.sh)** — the monthly entrypoint. Much shorter than `rto_ev_data_etl.sh`: no Selenium, no xvfb, no browser-runtime policy, no missing-file pass. Uses the same `ops/etl_runtime.sh` alerting.
- `tests/test_new_portal_rto_fetch.py`, registered in `ops/run_repo_checks.sh`. 134 tests pass overall.

### The call-strategy breakthrough

The plan in the call-volume section below assumed the monthly grain had to come from repeated per-month calls. It doesn't. **`durationWiseRegistrationTable` honours `vehicleClasses` and `vehicleFuels[]` together, and returns every calendar month of the requested year range in one response** (verified 2026-08-18). So:

1. `classdistribution` (year-scoped) → which vehicle classes this RTO reported at all.
2. `fueltypedonutchart` per non-zero class (year-scoped) → which fuels that class reported at all.
3. `durationWiseRegistrationTable` per non-zero (class, fuel) → the monthly split, **all 12 months in one call**.

Only combinations already known to be non-zero are ever queried. Pulling one month therefore costs exactly the same as pulling the whole year, which is why the pipeline re-pulls the year to date every run and replaces it wholesale — that is also what absorbs backdated registrations landing in already-ingested months (~0.5%, measured).

### Verification so far

Live end-to-end run against two Maharashtra RTOs for 2026 (484 rows):

- Summing across fuels for a class reproduces that class's own monthly totals **exactly**, every month.
- Every row's `total` equals the sum of its own fuel columns, by construction (`total` is summed from what was reported, never queried separately) — 0 of 484 rows disagreed.
- Aggregated per RTO, the output matched the independent server-computed `dashboardcount` tile at **+0.000%** — a stronger result than the ~0.5% Phase 0 drift, because both sides here are the same-day live portal rather than portal-vs-V1-DB. Re-verified 2026-08-19 after the decision-6 revision: still +0.000%, with half the rows and half the runtime.
- Incidentally confirmed the backdated-registration effect the year-wide replacement strategy exists to absorb: the same two RTOs' 2026 totals moved between the 2026-08-18 and 2026-08-19 runs (16,025 → 16,121 and 25,120 → 25,338) purely from a day's additional data landing against months already captured.

### Ingestion replacement scope

`["year", "status_scope", "state_code"]`, not V1's `["date"]` — a fetch produces the complete snapshot for the years and states it covers, so those rows are replaced wholesale. This follows the same reasoning as V1's own orphaned-row fix (`865fd039`): scope the delete to whatever the run actually replaces, so an office that legitimately reported nothing this run doesn't leave a stale row behind.

`state_code` was added 2026-08-19, before the first single-state ingestion, for the same reason Telangana's historical backfill scopes to `["date", "state"]`. The delete matches staging rows via `EXISTS`, so including `state_code` confines it to the states actually present in the run; without it, a `--states MH` fetch would have deleted **every other state's** rows for that year. A nationwide run is unaffected, since staging then holds every state. The residual gap is a whole state vanishing from the portal, whose rows would never be deleted — far less likely than a single RTO reporting nothing, which the scope does handle.

The corollary is that a **partial** run must never be ingested — `rto_fetch.py` exits non-zero and warns loudly if any RTO failed, and the shell entrypoint stops there rather than proceeding to ingestion.

### Column types — a deliberate departure from V1, flagged for review

V1 stores every column as `NVARCHAR(MAX)`, which forces the curated dbt models to `CAST(... AS INT)` every count — exactly what broke when a preprocessing bug started emitting `"12.0"` instead of `"12"` (CLAUDE.md's August 2026 remediation, item 2). The V2 tables use real types (`INT` for counts, `DATE` for `date`, sized `NVARCHAR` for dimensions). Decision 2's "flat, matching V1" was about table *shape*, and the shape here is still flat — this departs from V1 only on types. **Say so if you'd rather V2 match V1's `NVARCHAR(MAX)` convention;** it is a one-line change per column in a migration that hasn't been run yet.

### First production run (2026-08-19)

The migration is **applied**. `sql/migrations/2026-08-18_new_portal_rto_v2_tables.sql` ran against the production database on 2026-08-19, creating `dbo.fact_ev_data_by_rto_v2`, `dbo.staging_fact_ev_data_by_rto_v2` and the two indexes. There is no `sqlcmd` on the VM, so it was executed as a single pyodbc batch (the file contains no `GO` separators). Pre-flight confirmed both tables were absent beforehand; the migration's own guard clauses would have aborted otherwise.

Verified immediately after:
- Both tables have 51 columns in **exactly** the order `new_portal/schema.py` declares — the property the whole `INSERT INTO final SELECT * FROM staging` design rests on.
- Types landed as intended: 40 `int`, 9 `nvarchar`, 1 `date`, 1 `datetime`.
- V1 untouched: `fact_ev_data_by_rto` and `rto_wise_ev_data` both still at 2,291,901 rows.

Then a real fetch and ingest of **Mizoram, 2026** (10 RTOs, 411 rows, ~70s to fetch):

| Check | Result |
| --- | --- |
| Duplicate grain keys | 0 |
| Rows where `total` ≠ sum of fuel columns | 0 of 411 |
| Rows with a non-NULL portal-absent fuel | 0 (`bio_diesel_b100`/`bio_methane` correctly NULL throughout) |
| `status_scope` values present | `ALL_STATUSES` only |
| Per-RTO totals vs live `dashboardcount` | **+0.000% on all 9 reporting RTOs** |

Mizoram's 2026 totals: 22,491 registrations, of which 370 `pure_ev`.

The tenth RTO, `MZ/99` "STATE TRANSPORT AUTHORITY", returned zero rows. That is the expected shape for an administrative office with no vehicle registrations, and it exercised the empty-RTO path without error.

Note the ingested CSV was deleted from the VM afterwards along with the ad-hoc verification scripts, to avoid leaving untracked files on a machine this repo already treats as drift-prone. Re-running the fetch regenerates it.

### Crosswalk re-verified against the real database (2026-08-19)

`build_rto_crosswalk.py` was re-run on the VM with live DB access, closing the open item about the 2026-08-18 regeneration having used cached legacy names. Result: **all 1,686 rows, and every `state`, `state_code`, `new_rto_code`, `new_rto_name` and `link_status` value, identical.** The deterministic code rule (decision 4) is confirmed against the real `fact_ev_data_by_rto`, not just against a cached extract.

Only `legacy_rto_name` differed — which exposed a real determinism bug in the builder. It was selecting `DISTINCT state, rto_code, rto_name` and keeping whichever row arrived first, so for the ~two dozen offices with two historical names on file the winner was arbitrary: two runs over identical data produced 13 differing names, meaning every regeneration would churn the committed seed.

Fixed: the name from the most recent month of data now wins, with the name itself as a tiebreak — matching the "canonical name" convention the August 2026 dedup already established. Verified by running the builder twice and comparing checksums (identical). This updated 23 rows to their current names, exactly CLAUDE.md's known office-rename list — e.g. `DL7` `MAYUR VIHAR` → `EAST (MAYUR VIHAR)`, `MH16` `RTO AHEMEDNAGAR` → `RTO AHILYANAGAR`, `MZ9` `AIZAWL RURAL DTO` → `SAITUAL`.

**Watch out for V1's `date` column while doing this kind of work.** It is text in `DD/MM/YYYY` form, so a plain `CAST`/`TRY_CAST` silently reads it as `MM/DD/YYYY` — `'01/04/2026'` (1 April) becomes 4 January. The curated dbt models already handle this correctly with `CONVERT(DATE, [date], 103)`; **this is not a production bug**, it was checked. But ad-hoc SQL against V1 is exposed to it, which is why the builder's ordering uses the unambiguous `year`/`month` columns instead.

### Decision 10 — snapshot every fetched CSV to blob storage (decided 2026-08-20)

Earlier notes here suggested skipping blob upload for V2, reasoning that the CSV is reproducible from the API (unlike V1's XLSX, which was irreplaceable once Vahan closed). **Monish overrode that, with a better reason: the portal revises past months, so reproducibility is exactly the thing that isn't true.**

The evidence was already in hand — two RTOs' 2026 totals moved between consecutive daily runs (16,025 → 16,121 and 25,120 → 25,338) purely from backdated registrations landing against months already captured. Since ingestion replaces the year wholesale, the database only ever holds the latest pull. Without snapshots there is no way to answer "what did we believe last month, and how much has it moved?"

This also anticipates a planned change in cadence: **running several times a month** to capture the previous month repeatedly and track how it settles.

Implemented in **[`new_portal/rto_blob_snapshot.py`](/Users/monish/DataScraper_VahanParivahan/new_portal/rto_blob_snapshot.py)**, deliberately not reusing `etl_blob_upload.py`, because V1's helpers all pass `overwrite=True` — which would destroy precisely the history this exists to keep. Two rules follow:

1. **Never overwrite.** Uploads are `overwrite=False`; an existing blob is treated as already-snapshotted and left alone, so re-running is idempotent rather than destructive.
2. **The blob name carries the fetch time**, not just the year, or a several-runs-per-month cadence would collapse into one name. Layout: `year=2026/new_portal_rto_ev_data_2026__20260820T051701Z.csv` — Hive-style prefix so a year's snapshots list together and partition-aware tools can read the container.

The timestamp comes from the CSV's own mtime rather than upload time, so re-uploading the same artifact is genuinely idempotent. Container name is `new_portal_rto_csv_container_name` in `config.yaml` if present, else `new-portal-rto-csv` — a default so a fresh checkout works before anyone hand-edits the VM's config.

The entrypoint snapshots **before** ingesting, so a snapshot survives even if ingestion fails.

### Portal outage, 2026-08-20 — and what it exposed

The first attempted nationwide production run failed. Not our doing: `/analytics/*` returned **HTTP 503 on every path, from the VM and from an unrelated IP**, with a load balancer body reading `No server is available to handle this request`. Meanwhile `parivahan.gov.in` served 200 and `analytics.parivahan.gov.in/` itself returned 302 — so the host and load balancer were up and only the `/analytics` application backend pool was down. A portal-side outage, nothing to do but wait.

**What it exposed in our code was real, though.** The run produced 96 RTO failures that generated **401 HTTP requests** against an already-struggling service, because `portal_for_thread()` only cached the client *after* `start_session()` succeeded — so a failed session creation was never cached and every subsequent RTO retried it, three attempts each. Hammering a service that is telling you it is unavailable is exactly the wrong behaviour.

Two fixes, both tested:

- **Pre-flight.** One session is created before the worker fleet spawns. If that fails, the run exits immediately with a message naming the likely cause, turning a 400-request pile-up into a single failed request.
- **Circuit breaker.** `ABORT_AFTER_CONSECUTIVE_FAILURES = 25` stops a run whose failures are systemic rather than per-office, and the resulting CSV is explicitly flagged as un-ingestable.

## Not yet decided / next steps

- ~~Run the migration and a first single-state ingestion.~~ **Done 2026-08-19** — see "First production run".
- **Next: widen beyond one state.** Mizoram is 10 small RTOs; a large state (MH, UP, RJ) will exercise pagination-scale class/fuel variety and multi-hour session health in a way Mizoram cannot.
- ~~Decide how to handle the nationwide runtime.~~ **Solved 2026-08-19** — one session per worker, 8 workers, ~45 min projected. Still needs one real full-scale run before cron, to confirm eight sessions stay healthy for the whole run.
- **Route `Motorised Cycle (CC  25cc)` to whoever owns `Table and Mapping V2.xlsx`.** The new portal reports this vehicle_class and the mapping file doesn't have it, so it currently defaults to `Others`/`Others`/`Others` with a warning — decision 5's runtime rule working as intended, but a real gap to close upstream rather than patch here. (Note the doubled space in the portal's own label.)
- ~~Re-run `build_rto_crosswalk` on the VM against the real DB.~~ **Done 2026-08-19** — all code mappings identical; see "Crosswalk re-verified".
- Decide whether V2 needs a curated dbt model yet, and what it should look like given `status_scope`. Every query against the fact table **must** filter on `status_scope` or it double-counts.
- Periodically re-run `build_vehicle_class_crosswalk.py` as new months land, to catch any new `vehicle_class` the new portal reports that isn't in `Table and Mapping V2.xlsx` yet (decision 5) — the mapping file itself is authoritative and business-owned, so a coverage gap should be routed back to whoever maintains it, not silently patched around in this pipeline.
- Look at the remaining untested endpoints (`vahanyearwiseregistrationtrend`, `emissionTreeMap`, `vehicleRegistration`, `vahanYearWiseRegistrationComparisonChart`, `top5chart`). `statusdistribution` was tried on 2026-08-18 and returns an empty payload.
- OEM and State grains remain deferred until RTO proves the pattern in production.
