# New-Portal RTO (V2) Runbook

## Purpose

The V2 pipeline collects RTO-grain registration data from the new Parivahan
Analytics portal into `fact_ev_data_by_rto_v2`. It is a JSON API, not Selenium:
no browser, no `xvfb`, no `VAHAN_HEADLESS`, no missing-file recovery pass and no
XLSX preprocessing.

**One portal call returns a whole calendar year.** The unit of work is therefore
a year, not a month, and every run re-pulls the year to date and replaces it
wholesale. That is what absorbs backdated registrations landing in months that
were already ingested, and it is why "last month" is never an input to the
pipeline — only an assertion about its output.

## Entry Point

- Cron: [`new_portal_rto_monthly.sh`](/Users/monish/DataScraper_VahanParivahan/new_portal_rto_monthly.sh)
- By hand, for one month: `bash new_portal_rto_monthly.sh 2026 10`

**Do not use `new_portal_rto_etl.sh` for a full run.** It makes a single fetch
attempt at the unsafe default pacing, which cannot complete a nationwide year,
and its first stage is a fetch — so running it after a successful sweep re-pulls
from zero and overwrites the complete CSV. Measured 2026-11-04: a paced sweep
finished 2026 in 1h29m with zero gaps; the ETL script invoked immediately after
re-fetched for 87 minutes, overwrote the CSV with 123,693 rows and 314 gaps, and
failed. It remains useful only as a single-state smoke test.

## Stages

| Step | What it does | On failure |
|---|---|---|
| `fetch_sweep` | `ops/new_portal_sweep_until_complete.sh`, paced at `SLEEP_SECONDS=0.45`, `MAX=30 WAIT=300 STALL_LIMIT=6` | Nothing was ingested. Safe to re-run; it resumes from the per-office checkpoint. |
| `completeness_gate` | `new_portal.rto_completeness` — refuses to proceed unless the artifacts prove a complete, fresh, nationwide run covering the target month | Nothing was ingested. Read the logged failures; they name the reason. |
| `blob_snapshot` | `new_portal.rto_blob_snapshot` — immutable per-run CSV snapshot, **before** ingestion | The CSV is still on disk. Re-run the snapshot and ingest stages only. |
| `ingestion` | `new_portal.rto_bulk_ingest`, falling back to `new_portal.rto_ingest` | A snapshot exists; re-run ingest only. **See "If ingestion was killed" below.** |
| `post_ingest_check` | `ops/check_pipeline_freshness.py` | The data is loaded; the verification failed. Read which of the two predicates fired. |
| `staging_cleanup` | `ops/truncate_v2_scratch_tables.py` | Harmless; nothing reads those tables between runs. |

There is **no dbt stage**, because no curated V2 model exists. Do not add one.

## Schedule

See [`ops/production_vm.crontab`](/Users/monish/DataScraper_VahanParivahan/ops/production_vm.crontab),
which carries the full reasoning. In short:

- **Day 3, 03:30 UTC** — primary run. Day 3 rather than day 1 because the V1
  block clears by ~00:40 UTC on day 2 (measured: State ~4 min, OEM ~1h51m, RTO
  ~1h10m), and because the portal revises past months, so two extra days of
  settling is worth having.
- **Day 5, 03:30 UTC** — retry. It resumes from the checkpoint rather than
  restarting, so it is cheap when day 3 succeeded and recovers the month when
  day 3 hit an outage.
- **Daily, 06:15 UTC** — on-VM load-recency check.
- **Daily, 06:00 UTC, off-VM** — `.github/workflows/freshness_watchdog.yml`.

**Coupling to watch:** `--max-load-age-days 35` is derived from the day-5 retry
(day 3 of a 31-day month to day 5 of the next = 33 days, plus two days of
buffer). **If the retry day moves, that threshold moves with it.**

**Hard external dependency:** the VM is off by default and is started by
external automation not represented in this repo. The schedule needs it up
03:00–07:00 UTC on days 3 and 5. A cron line on a powered-off machine does
nothing, which is exactly the failure this whole design exists to catch rather
than prevent.

## What each alert means

| Alert | Meaning | What to do |
|---|---|---|
| `new_portal_rto` FAILED at `fetch_sweep` | The sweep could not complete the year. Nothing ingested. | Re-run; it resumes. If it dies at ~205 offices every attempt, the portal's rate limit moved — re-measure with ≥150s of sustained load before changing `SLEEP_SECONDS`. |
| `new_portal_rto` FAILED at `completeness_gate` | The fetch output could not be proven complete. **Nothing ingested — this is the gate working.** | Read the named failures and re-run. |
| `new_portal_rto` FAILED at `ingestion` | A snapshot exists; the load did not finish. | Re-run ingestion only. Check "If ingestion was killed". |
| `new_portal_rto_freshness` FAILED on **load recency** | No run has landed. The VM was up enough to run the check, so the schedule or the pipeline is the problem. | Investigate the schedule; run the month by hand. |
| `new_portal_rto_freshness` FAILED on **month settled** | A run landed but captured an in-flight month. | Re-run now that the month is closed. |
| `new_portal_rto_snapshot_watchdog` FAILED | **No run happened at all.** This check runs off the VM. | Check whether the VM started. |

## Why `MAX([date])` is not a health signal

The worked example is September 2026. Every V2 run writes rows for the in-flight
current month, so a run on 2026-09-24 left `MAX([date])` = September with six
days of the month still unelapsed: 15,049 rows and 239,579 EV against V1's
settled 332,523, a −27.95% shortfall. October then never ran at all. Through all
of that, `MAX([date])` equalled "the previous calendar month" and any check built
on it would have reported healthy. The staleness signal is `MAX([inserted_at])`.

And the settled check compares `SUM([total])`, not `COUNT(*)`. A partial capture
keeps full breadth — every state, every office reporting — and shrinks only the
counts inside each row, so row count barely moves: the September stub sits at
0.9255 month-over-month against a healthy band of 0.9818–1.0210. Registrations
separate it at 0.7763 against a healthy 0.8878–1.1162.

**The 0.85 floor is thin and is not comfortable.** The healthy minimum observed
is 0.8878 and the stub is 0.7763 — a 1.1-point gap, on seven month-over-month
samples from a single year. A genuinely weak trading month could trip it.
**Re-check the band after each of the first two scheduled runs** and record the
updated range here with its date and sample size.

| Recorded | Healthy range (`SUM([total])` MoM) | Samples | Floor |
|---|---|---|---|
| 2026-11-03 | 0.8878 – 1.1162 | 7 (2026 months 2–8) | 0.85 |

## How to re-run one month by hand

```bash
ssh <vm>
cd /home/climate_dot_data/DataScraper_VahanParivahan
setsid nohup bash new_portal_rto_monthly.sh 2026 10 \
  >> new_portal_rto_monthly_logs.txt 2>&1 &
```

Budget 2–5 hours. Progress: `YEAR=2026 PYTHONPATH=. ./venv/bin/python ops/new_portal_progress_count.py`.

## Rollback

**Database.** Ingestion replaces the year wholesale, scoped on
`(year, status_scope, state_code)`:

```sql
DELETE FROM dbo.fact_ev_data_by_rto_v2
WHERE [year] = <Y> AND [status_scope] = 'ALL_STATUSES';
```

Then re-ingest from the immutable blob snapshot of the last known-good run — that
is what the snapshots are for. Snapshots live in the `new-portal-rto-csv`
container under `year=<Y>/`, named with the CSV's own mtime.

**If ingestion was killed** (for example the VM powered off mid-load), the year
may be deleted-but-not-reinserted, and `staging_cleanup` will not have run — so
staging still holds the rows that would repair it. Check row counts per year
before doing anything else.

**Never run `build_delete_query()` or `INSERT INTO final SELECT * FROM staging`
by hand without first checking what year staging holds.** Those statements are
correct only immediately after staging was repopulated. Between runs the scratch
tables hold whatever was loaded last — measured 2026-11-16, 169,083 rows of
2026 — so running them by hand would delete and re-insert a year nobody intended
to touch. `ops/truncate_v2_scratch_tables.py --dry-run` tells you what is in
there.

**Crontab.** The live crontab and `ops/production_vm.crontab` are separate
sources of truth with no automation syncing them, so a rollback needs both:
`crontab ~/crontab_backup_<YYYYMMDD>.txt` and `git revert` the commit.

## Known gaps

- **429 backoff is unimplemented.** `client.py` treats 429 like any other
  non-200 and `rto_fetch` sets `retry_attempts=1`, so a throttle window is
  indistinguishable from a dead office and feeds the circuit breaker.
  `SLEEP_SECONDS=0.45` is a workaround that assumes the bucket stays where it
  was measured on 2026-11-03. The real fix is for `client.py` to treat 429 as
  its own case and not count it as a gap.
- **Whether a GitHub runner can reach the Azure SQL server is unknown**, so the
  off-VM watchdog watches blob age as a proxy. If SQL turns out to be reachable,
  an off-VM `MAX([inserted_at])` watchdog is strictly stronger.
- **GitHub Actions is disabled for this repository**, so the off-VM watchdog
  cannot fire until Actions is enabled and the two repository secrets
  (`AZURE_STORAGE_CONNECTION_STRING`, `GOOGLE_CHAT_WEBHOOK_URL`) are set.
- **If `storage.new_portal_rto_csv_container_name` is ever added to
  `config.yaml`**, the pipeline starts writing to a different container and the
  off-VM watchdog keeps reading the abandoned one, reporting healthy forever.
  Update `ops/check_snapshot_freshness.py` on the same day.
