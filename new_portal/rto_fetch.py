"""Fetches a full year of RTO-grain registration data from the new portal and
writes it as a CSV ready for `new_portal/rto_ingest.py`.

This replaces V1's scrape + missing-file-recovery + preprocessing steps in one
go: the new portal has no file download, so there is nothing to recover and
nothing to parse out of XLSX.

## Why a year at a time, not a month

`durationWiseRegistrationTable` returns **one row per calendar month for the
whole requested year range in a single call** (verified 2026-08-12). So pulling
one month costs exactly as much as pulling twelve. The monthly job therefore
re-pulls the current year to date and replaces it wholesale, which also absorbs
backdated registrations landing in already-ingested months — a real effect,
measured at roughly +0.5% on a month that had already been captured.

## Call strategy

Naively this is (vehicle classes x fuels) calls per RTO. Two year-scoped
"what actually has data?" probes cut that down hard:

1. `classdistribution` -> which vehicle classes this RTO reported at all.
2. `fueltypedonutchart` per class -> which fuels that class reported at all.
3. `durationWiseRegistrationTable` per (class, fuel) -> the monthly split.

Only step 3's combinations are ever issued, and only for combinations already
known to be non-zero. Verified 2026-08-18 on Pune/2026: summing step 3 across
fuels reproduces the class's own monthly totals exactly, for every month.

## Status scope

Only ALL_STATUSES is pulled (decision 6, revised 2026-08-19) — see
new_portal/schema.py for why. `--status-scopes` can re-enable ACTIVE, at the
cost of roughly doubling the run.

Usage:
    python3 -m new_portal.rto_fetch                  # current year, all RTOs
    python3 -m new_portal.rto_fetch --year 2026
    python3 -m new_portal.rto_fetch --year 2026 --states MH,GJ
    python3 -m new_portal.rto_fetch --year 2026 --limit-rtos 5   # smoke test
    python3 -m new_portal.rto_fetch --status-scopes ALL_STATUSES,ACTIVE
"""

from __future__ import annotations

import argparse
import collections
import csv
import dataclasses
import itertools
import json
import logging
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from requests.adapters import HTTPAdapter

from pipeline_constants import STATE_NAME_TO_NEW_PORTAL_CODE

from new_portal.client import (
    ARCHIVE_TYPES_ACTIVE_ONLY,
    ARCHIVE_TYPES_ALL_STATUSES,
    NewPortalClient,
    NewPortalError,
)
from new_portal.schema import (
    CSV_COLUMNS,
    DEFAULT_STATUS_SCOPES,
    FUEL_LABEL_TO_COLUMN,
    STATUS_SCOPE_ACTIVE,
    STATUS_SCOPE_ALL,
    STATUS_SCOPES,
    parse_year_as_string,
)

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent
RTO_CROSSWALK_PATH = REPO_ROOT / "climate_dot_dbt" / "seeds" / "rto_code_crosswalk.csv"
VEHICLE_CLASS_CROSSWALK_PATH = (
    REPO_ROOT / "climate_dot_dbt" / "seeds" / "vehicle_class_crosswalk.csv"
)

FILE_PREFIX = "new_portal_rto_ev_data"

ARCHIVE_TYPES_BY_SCOPE = {
    STATUS_SCOPE_ACTIVE: ARCHIVE_TYPES_ACTIVE_ONLY,
    STATUS_SCOPE_ALL: ARCHIVE_TYPES_ALL_STATUSES,
}

# Mirrors V1's own fallback for a vehicle_class absent from the business mapping
# file (etl_preprocessing.DEFAULT_DIMENSION_VALUE). Decision 5 requires the
# runtime path to degrade the same way the crosswalk builder does.
DEFAULT_DIMENSION_VALUE = "Others"

# Deliberate courtesy pacing against a live government service. The portal's
# rate limit is tied to session creation, not call volume (a 26-call burst
# against one warm session saw zero throttling), so this is not a technical
# requirement — but a run at this scale should not hammer.
DEFAULT_SLEEP_SECONDS = 0.2

# RTOs are independent, so the run parallelises across them. Each RTO's own
# class -> fuel -> month sequence stays serial, which keeps the per-RTO logic
# and its error handling exactly as it is when run single-threaded.
#
# CRITICAL: each worker needs its OWN session. The portal serializes concurrent
# requests sharing a JSESSIONID (it is a Java app), so threads sharing one
# session gain almost nothing. Measured on the VM 2026-08-19 over a real 48-RTO
# fetch: 1 worker 564s, 8 workers 463s, 16 workers 459s — flat past 8 because
# the session was the bottleneck, not the server. Giving each worker its own
# session on the same workload ran 11.9x faster (32.4s -> 2.7s).
#
# Throughput then scales with SESSION COUNT, at roughly 4 calls/sec per session:
# 2 sessions -> 8.8/s, 4 -> 17.5/s, 8 -> 25.8/s, all with zero HTTP 429s.
# 8 sessions with a small per-call sleep was the fastest configuration that
# provoked no throttling at all; pushing harder (8 sessions, no sleep) did
# trigger 429s, which retries papered over but which is not a design to ship.
DEFAULT_WORKERS = 8

# The breaker exists to stop a run when the portal is not serving data at all,
# without stopping one that is merely having a bad day. It was added after
# 2026-08-20, when the /analytics backend went down mid-run and 96 failed RTOs
# generated 401 requests against an already-struggling service.
#
# "N consecutive failures" cannot tell those apart. Failures on this portal are
# per-office and clustered — an office's outcome is stable within a window
# (measured 3/3 identical across 8 offices) and flips over minutes — so a small
# state whose offices all fail together produces a long consecutive run while
# the portal is still serving most of the country. That aborted a run at office
# 133 which had already collected 5,072 rows from 71 healthy offices.
#
# A rolling failure rate distinguishes them: a dead portal fails ~everything, a
# degraded one fails a fraction.
#
# The window is wide on purpose. Availability here is per-office and rotates
# over minutes, and the sweep visits offices grouped by state, so an entire
# region can be unavailable while the rest of the country serves normally —
# measured 2026-09-24: HP/HR offices failing 29 of 32 while Mumbai, Lucknow and
# Chennai answered every request. A 50-office window treated that as a dead
# portal and abandoned a sweep whose later regions were fine.
#
# 200 offices at 95% is narrow enough that only a portal-wide outage trips it,
# and wide enough that no single region can. The cost of being wrong is small:
# failing offices are fast, so 200 of them is around two minutes of light load,
# and an abort no longer loses anything now that runs resume.
ABORT_WINDOW = 200
ABORT_MIN_SAMPLE = 100
ABORT_FAILURE_RATE = 0.95

# How long the recovery pass waits before retrying gapped offices. The portal's
# intermittent 500s clear over minutes, not seconds (measured 2026-09-23), so
# retrying immediately mostly re-hits the same errors.
RECOVERY_SETTLE_SECONDS = 120.0

# In-line retries are deliberately off by default, which is not the obvious
# choice, so: the client's own 3-attempt / 5s-linear-backoff policy costs ~15s
# per failed call and measurably does not work on this portal — 3 attempts moved
# a Mizoram sweep from 13 failures to 11. In a degraded nationwide run that
# bought ~nothing for ~42% of wall-clock time (434 gaps x 15s across 8 workers,
# against 32 minutes elapsed).
#
# The recovery pass is where retrying actually pays, because it waits minutes
# and rebuilds the session rather than hammering the same one within seconds.
# So the main pass fails fast and hands the work to recovery.
DEFAULT_RETRY_ATTEMPTS = 1


def build_output_path(year: int, output_dir: Path | None = None) -> Path:
    directory = output_dir or REPO_ROOT
    return directory / f"{FILE_PREFIX}_{year}.csv"


def load_rto_targets(
    crosswalk_path: Path = RTO_CROSSWALK_PATH,
    *,
    states: list[str] | None = None,
) -> list[dict]:
    """Every RTO V2 should ingest, from the live-portal-driven crosswalk.

    All rows with a `new_rto_code` are included regardless of `link_status`:
    the 277 `portal_only` offices have no V1 history but are real offices
    reporting real registrations (decided with Monish 2026-08-18). The 10
    `legacy_only` rows have no portal code and are skipped — there is nothing
    to fetch for them.
    """
    targets = []
    with crosswalk_path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if not row["new_rto_code"]:
                continue
            if states and row["state_code"] not in states:
                continue
            targets.append(row)
    return targets


STAGE_CLASS_DISTRIBUTION = "class_distribution"
STAGE_FUEL_BREAKDOWN = "fuel_breakdown"
STAGE_MONTHLY = "monthly"


@dataclasses.dataclass(frozen=True)
class Gap:
    """One unit of data the portal would not serve.

    A gap is deliberately *not* an absent row. An RTO that reports 19 vehicle
    classes and hands us 18 has under-counted totals, and a missing row would
    read downstream as a genuine zero. Naming the hole lets the recovery pass
    retry exactly it, and lets the run refuse to be ingested while any remain.

    `stage` records how much was lost, because the blast radius differs:
      - class_distribution: the whole (RTO, scope). No classes could be listed.
      - fuel_breakdown:     one class. Its fuels could not be listed.
      - monthly:            one (class, fuel) cell.
    """

    state_code: str
    rto_code: int
    rto_name: str
    status_scope: str
    stage: str
    vehicle_class: str | None = None
    fuel_label: str | None = None
    error: str = ""

    @property
    def rto_label(self) -> str:
        return f"{self.state_code}/{self.rto_code} {self.rto_name}"

    def describe(self) -> str:
        parts = [self.rto_label, self.status_scope, self.stage]
        if self.vehicle_class:
            parts.append(repr(self.vehicle_class))
        if self.fuel_label:
            parts.append(repr(self.fuel_label))
        return " | ".join(parts)


def normalize_class_label(label: str) -> str:
    """"Motorised Cycle (CC  25cc)" and "MOTORISED CYCLE (CC > 25CC)" -> one key.

    The portal and V1 disagree about the *spelling* of classes they both
    report, in two ways that are presentation, not meaning:

    1. Case — the portal renders "Motor Car", V1 stores "MOTOR CAR".
    2. Punctuation and spacing — the portal's "Motorised Cycle (CC  25cc)" has
       lost the ">" and gained a doubled space relative to V1's
       "MOTORISED CYCLE (CC > 25CC)".

    Dropping everything except letters and digits collapses both. Verified
    collision-free across all 76 classes in the crosswalk (2026-09-23): 76
    labels produce 76 distinct keys, so this cannot silently merge two real
    classes.
    """
    return re.sub(r"[^A-Z0-9]+", "", label.upper())


def load_vehicle_class_dimensions(
    crosswalk_path: Path = VEHICLE_CLASS_CROSSWALK_PATH,
) -> dict[str, tuple[str, str, str, str]]:
    """normalized label -> (canonical_class, type, category, use_type).

    Keyed on `normalize_class_label` so the portal's spelling finds V1's row,
    and carries the crosswalk's own `vehicle_class` as the canonical name so
    callers can write V1's spelling rather than the portal's.
    """
    dimensions = {}
    with crosswalk_path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            canonical = row["vehicle_class"].strip()
            dimensions[normalize_class_label(canonical)] = (
                canonical,
                row["vehicle_type"],
                row["vehicle_category"],
                row["vehicle_use_type"],
            )
    return dimensions


def lookup_dimensions(
    vehicle_class: str, dimensions: dict[str, tuple[str, str, str, str]]
) -> tuple[str, str, str, str]:
    """Returns (canonical_class, type, category, use_type).

    The canonical class is V1's spelling, so `fact_ev_data_by_rto_v2` joins to
    `fact_ev_data_by_rto` on `vehicle_class` without per-query normalization.
    An unknown class keeps the portal's own label — inventing a canonical name
    for it would be worse than recording what the source actually said.
    """
    resolved = dimensions.get(normalize_class_label(vehicle_class))
    if resolved is None:
        logger.warning(
            "vehicle_class %r is not in the crosswalk; defaulting its dimensions "
            "to %r. If this class is real and recurring, it belongs in "
            "'Table and Mapping V2.xlsx' — route it to whoever owns that file "
            "rather than patching it here.",
            vehicle_class,
            DEFAULT_DIMENSION_VALUE,
        )
        return (vehicle_class.strip(), *((DEFAULT_DIMENSION_VALUE,) * 3))
    return resolved


def _nonzero_labels(breakdown: dict) -> list[str]:
    """{"labels": [...], "data": [...]} -> labels whose count is non-zero."""
    labels = breakdown.get("labels") or []
    data = breakdown.get("data") or []
    return [label for label, count in zip(labels, data) if count]


def fetch_rto_year(
    portal: NewPortalClient,
    rto: dict,
    year: int,
    vehicle_class_dimensions: dict[str, tuple[str, str, str]],
    *,
    status_scopes: list[str] | None = None,
    sleep_seconds: float = DEFAULT_SLEEP_SECONDS,
    sleep_func=time.sleep,
) -> tuple[list[dict], list[Gap]]:
    """All CSV rows for one RTO for one year, plus whatever the portal refused.

    Defaults to ALL_STATUSES only (see schema.DEFAULT_STATUS_SCOPES). Each extra
    scope multiplies the API calls for this RTO, since no endpoint returns more
    than one archive-status scope per request.

    ## Why failures are isolated per call rather than per RTO

    The portal intermittently 500s on individual sub-queries — measured
    2026-09-23, a drifting subset of (RTO, class) pairs, unrelated to request
    rate, session age or which parameters change. Letting one of those abort the
    whole office cost us 7 of 10 Mizoram offices in a run where most classes
    were perfectly fetchable.

    So a `NewPortalError` on one unit records a `Gap` and moves on. Any other
    exception propagates: a KeyError in our own parsing is a bug, and silently
    filing it as "the portal wouldn't serve this" would hide it.
    """
    state_code = rto["state_code"]
    rto_code = int(rto["new_rto_code"])
    scope_args = dict(
        state_code=state_code, rto_code=rto_code, from_year=year, to_year=year
    )

    rows: list[dict] = []
    gaps: list[Gap] = []

    def gap(status_scope, stage, error, vehicle_class=None, fuel_label=None) -> None:
        gaps.append(
            Gap(
                state_code=state_code,
                rto_code=rto_code,
                rto_name=rto["new_rto_name"],
                status_scope=status_scope,
                stage=stage,
                vehicle_class=vehicle_class,
                fuel_label=fuel_label,
                error=str(error),
            )
        )

    for status_scope in status_scopes or DEFAULT_STATUS_SCOPES:
        archive_types = ARCHIVE_TYPES_BY_SCOPE[status_scope]
        # (month, vehicle_class) -> {fuel_column: count}
        cells: dict[tuple[int, int, str], dict[str, int]] = {}

        try:
            class_breakdown = portal.get_class_distribution(
                **scope_args, archive_types=archive_types
            )
        except NewPortalError as exc:
            # Nothing else in this scope is reachable without the class list.
            gap(status_scope, STAGE_CLASS_DISTRIBUTION, exc)
            continue
        sleep_func(sleep_seconds)

        for vehicle_class in _nonzero_labels(class_breakdown):
            try:
                fuel_breakdown = portal.get_fuel_type_breakdown(
                    **scope_args,
                    vehicle_classes=vehicle_class,
                    archive_types=archive_types,
                )
            except NewPortalError as exc:
                gap(
                    status_scope,
                    STAGE_FUEL_BREAKDOWN,
                    exc,
                    vehicle_class=vehicle_class,
                )
                continue
            sleep_func(sleep_seconds)

            for fuel_label in _nonzero_labels(fuel_breakdown):
                fuel_column = FUEL_LABEL_TO_COLUMN.get(fuel_label)
                if fuel_column is None:
                    logger.warning(
                        "Unknown fuel label %r from the portal (%s/%s, class %r); "
                        "its counts are being dropped. Add it to "
                        "new_portal/schema.PORTAL_FUEL_LABELS and to the V2 table.",
                        fuel_label,
                        state_code,
                        rto_code,
                        vehicle_class,
                    )
                    continue

                try:
                    monthly = portal.get_duration_wise_registration(
                        **scope_args,
                        vehicle_classes=vehicle_class,
                        vehicle_fuels=[fuel_label],
                        archive_types=archive_types,
                    )
                except NewPortalError as exc:
                    gap(
                        status_scope,
                        STAGE_MONTHLY,
                        exc,
                        vehicle_class=vehicle_class,
                        fuel_label=fuel_label,
                    )
                    continue
                sleep_func(sleep_seconds)

                for entry in monthly:
                    count = entry.get("registeredVehicleCount") or 0
                    if not count:
                        continue
                    entry_year, month = parse_year_as_string(entry["yearAsString"])
                    cell = cells.setdefault((entry_year, month, vehicle_class), {})
                    cell[fuel_column] = cell.get(fuel_column, 0) + int(count)

        for (entry_year, month, vehicle_class), fuel_counts in sorted(cells.items()):
            rows.append(
                _build_row(
                    rto=rto,
                    year=entry_year,
                    month=month,
                    vehicle_class=vehicle_class,
                    status_scope=status_scope,
                    fuel_counts=fuel_counts,
                    vehicle_class_dimensions=vehicle_class_dimensions,
                )
            )

    return rows, gaps


def _build_row(
    *,
    rto: dict,
    year: int,
    month: int,
    vehicle_class: str,
    status_scope: str,
    fuel_counts: dict[str, int],
    vehicle_class_dimensions: dict[str, tuple[str, str, str, str]],
) -> dict:
    # `canonical_class` is V1's spelling of the same class, not the portal's —
    # see lookup_dimensions.
    canonical_class, vehicle_type, vehicle_category, vehicle_use_type = lookup_dimensions(
        vehicle_class, vehicle_class_dimensions
    )
    row = {column: "" for column in CSV_COLUMNS}
    row.update(
        {
            "year": year,
            "month": month,
            "date": f"{year:04d}-{month:02d}-01",
            "state": rto["state"],
            "state_code": rto["state_code"],
            "rto_code": int(rto["new_rto_code"]),
            "rto_name": rto["new_rto_name"],
            "legacy_rto_code": rto["legacy_rto_code"],
            "vehicle_class": canonical_class,
            "vehicle_type": vehicle_type,
            "vehicle_category": vehicle_category,
            "vehicle_use_type": vehicle_use_type,
            "status_scope": status_scope,
            # Sum of what the portal actually reported, not an independent
            # "all fuels" query — so `total` is always internally consistent
            # with the fuel columns beside it, by construction.
            "total": sum(fuel_counts.values()),
        }
    )
    row.update(fuel_counts)
    return row


def should_abort(recent_outcomes) -> bool:
    """Whether the recent failure rate means the portal has stopped serving.

    `recent_outcomes` is an iterable of booleans, True for a failed office, over
    a rolling window of the most recent offices. Returns True once the window is
    almost entirely failures.

    The threshold is deliberately high. A degraded portal that still answers a
    third of offices is worth continuing against — that run collects real rows
    and the recovery pass mops up the rest. Only near-total failure means there
    is nothing to be gained by carrying on.
    """
    outcomes = list(recent_outcomes)
    if not outcomes:
        return False
    return sum(outcomes) / len(outcomes) >= ABORT_FAILURE_RATE


def office_counts_as_failure(rows: list[dict], gaps: list[Gap]) -> bool:
    """Whether one office's result should count toward the circuit breaker.

    Three outcomes, only one of which is a failure:

    - **rows, with or without gaps** — the office answered. Success, even if
      partial; the recovery pass owns the gaps.
    - **no rows, no gaps** — the office genuinely reported nothing this year.
      Common and legitimate: the first 13 offices in the national list are small
      Andaman islands with no 2026 registrations at all. Counting these would
      abort a perfectly healthy run before it reached the mainland.
    - **no rows, and gaps** — every call we made failed. Indistinguishable from
      the hard failure the breaker exists for, so it counts.

    The middle case is why "did it return rows?" is not the right test on its
    own, and the last is why "did it return at all?" is not either.
    """
    return not rows and bool(gaps)


def build_progress_path(year: int, output_dir: Path | None = None) -> Path:
    csv_path = build_output_path(year, output_dir)
    return csv_path.with_name(f"{csv_path.stem}__progress.jsonl")


def load_progress(path: Path) -> dict[str, tuple[list[dict], list[Gap]]]:
    """{rto_label: (rows, gaps)} from a previous run's checkpoint.

    Later lines win, so re-attempting an office simply appends a better result.
    A truncated final line — the run was killed mid-write — is skipped rather
    than aborting the load; losing one office's checkpoint costs one re-fetch.
    """
    if not path.exists():
        return {}

    progress: dict[str, tuple[list[dict], list[Gap]]] = {}
    with path.open(encoding="utf-8") as f:
        for line_number, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
                progress[entry["rto_label"]] = (
                    entry["rows"],
                    [Gap(**g) for g in entry["gaps"]],
                )
            except (json.JSONDecodeError, KeyError, TypeError):
                logger.warning(
                    "Skipping unreadable checkpoint line %s in %s; that office "
                    "will be re-fetched.",
                    line_number, path,
                )
    return progress


def append_progress(path: Path, rto_label: str, rows: list[dict], gaps: list[Gap]) -> None:
    """Record one office's result. Append-only so a kill cannot corrupt earlier lines."""
    path.parent.mkdir(parents=True, exist_ok=True)
    entry = {
        "rto_label": rto_label,
        "rows": rows,
        "gaps": [dataclasses.asdict(g) for g in gaps],
    }
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")
        f.flush()


def office_is_complete(entry: tuple[list[dict], list[Gap]]) -> bool:
    """Whether a checkpointed office needs no further work.

    Complete means "no gaps" rather than "has rows", because a genuinely empty
    office is finished too. Anything with gaps is re-attempted on the next run,
    which folds resume and recovery into the same mechanism: a later window
    gets a fresh chance at exactly the offices that were short.
    """
    _, gaps = entry
    return not gaps


def is_better_result(
    new_rows: list[dict],
    new_gaps: list[Gap],
    previous: tuple[list[dict], list[Gap]] | None,
) -> bool:
    """Whether a fresh attempt should replace what the checkpoint holds.

    Fewer gaps wins. On an equal gap count, more rows wins — and that tie-break
    is the whole point of this function rather than a bare `<=`.

    Two attempts can gap the same *number* of calls while gapping *different*
    ones, because availability is per-office and rotates. Replacing on `<=`
    therefore let a retry that happened to lose bigger classes overwrite a
    better earlier pull: observed 2026-09-24, a sweep over already-covered
    offices moved the dataset from 16,931 rows to 16,578 while gaps barely
    changed. While sweeps were still extending coverage the losses were masked
    by new offices; once coverage completed, every sweep was re-attempts and the
    loss would have dominated.
    """
    if previous is None:
        return True
    previous_rows, previous_gaps = previous
    if len(new_gaps) != len(previous_gaps):
        return len(new_gaps) < len(previous_gaps)
    return len(new_rows) > len(previous_rows)


def order_pending(targets, progress, done, label_of) -> list[dict]:
    """Offices to attempt, never-visited ones first.

    Ordering matters more than it looks. `pending` was simply "everything not
    complete", in the crosswalk's own order, so every attempt re-walked the same
    early gapped offices and spent its healthy window there. Measured across two
    attempts on 2026-09-23: the second attempted 385 offices and only 21 of them
    were new, leaving offices past ~450 untouched no matter how many attempts
    ran.

    Visiting unexplored offices first means each window extends coverage, and
    gapped offices are retried with whatever time is left. Both still get
    attempted in one pass; only the priority differs.
    """
    unvisited = [rto for rto in targets if label_of(rto) not in progress]
    gapped = [
        rto
        for rto in targets
        if label_of(rto) in progress and label_of(rto) not in done
    ]
    return unvisited + gapped


def build_gap_manifest_path(year: int, output_dir: Path | None = None) -> Path:
    csv_path = build_output_path(year, output_dir)
    return csv_path.with_name(f"{csv_path.stem}__gaps.json")


def write_gap_manifest(gaps: list[Gap], year: int, manifest_path: Path) -> None:
    """Persist the gaps beside the CSV.

    The CSV alone cannot express "this office is missing one class" — an absent
    row is indistinguishable from a real zero. The manifest is what makes a
    partial pull auditable after the fact instead of a silent under-count.
    """
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "year": year,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "gap_count": len(gaps),
        "affected_rtos": sorted({g.rto_label for g in gaps}),
        "gaps": [dataclasses.asdict(g) for g in gaps],
    }
    with manifest_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
        f.write("\n")


def recover_gaps(
    gaps: list[Gap],
    targets_by_label: dict[str, dict],
    year: int,
    vehicle_class_dimensions: dict[str, tuple[str, str, str, str]],
    *,
    client_factory,
    status_scopes: list[str] | None = None,
    sleep_seconds: float = DEFAULT_SLEEP_SECONDS,
    sleep_func=time.sleep,
    settle_seconds: float = RECOVERY_SETTLE_SECONDS,
    workers: int = DEFAULT_WORKERS,
) -> tuple[dict[str, list[dict]], list[Gap]]:
    """Second pass over the offices that came back with gaps.

    Returns ({rto_label: rows}, remaining_gaps). Only labels that genuinely
    improved appear in the dict; the caller keeps its first-pass rows otherwise.

    ## Why re-fetch the whole office rather than just the failed calls

    Rows are assembled from a (month, class) -> {fuel: count} accumulator, so
    splicing a single recovered fuel back into rows already written means
    rebuilding that office's cells anyway. Re-fetching the office is a handful
    of extra calls for the small minority that failed, and it removes a class of
    merge bug entirely.

    ## Why a fresh session, and why the wait

    Measured 2026-09-23: a label that 500d twice in a row recovered ~20 minutes
    later untouched, while retrying within seconds barely helped (3 attempts cut
    failures 13 -> 11). The failures clear on a timescale of minutes, so the
    recovery pass waits before starting and builds new sessions rather than
    reusing the ones that just saw errors.

    ## Why it runs in parallel

    This was sequential on one session at first, on the assumption that gapped
    offices would be a minority. On a degraded portal they are not: a nationwide
    sample had 83% of offices carrying at least one gap, which would have meant
    re-fetching ~1,391 offices one at a time — roughly 8 hours, longer than the
    main pass it was meant to patch up. It now parallelises the same way the
    main pass does, with one session per worker, since the portal serializes
    concurrent requests that share a JSESSIONID.
    """
    if not gaps:
        return {}, []

    labels = sorted({g.rto_label for g in gaps})
    logger.info(
        "Recovery pass: %s gap(s) across %s office(s); waiting %ss for the "
        "portal to settle before retrying with %s worker(s).",
        len(gaps), len(labels), settle_seconds, workers,
    )
    sleep_func(settle_seconds)

    gaps_by_label: dict[str, list[Gap]] = {}
    for g in gaps:
        gaps_by_label.setdefault(g.rto_label, []).append(g)

    thread_state = threading.local()

    def portal_for_thread() -> NewPortalClient:
        client = getattr(thread_state, "client", None)
        if client is None:
            client = client_factory()
            thread_state.client = client
        return client

    def recover_one(label: str):
        """Returns (label, rows_or_None, gaps_to_keep)."""
        rto = targets_by_label.get(label)
        if rto is None:
            # Should not happen; keep the gap rather than dropping it silently.
            return label, None, gaps_by_label[label]

        before = len(gaps_by_label[label])
        try:
            rows, retry_gaps = fetch_rto_year(
                portal_for_thread(),
                rto,
                year,
                vehicle_class_dimensions,
                status_scopes=status_scopes,
                sleep_seconds=sleep_seconds,
                sleep_func=sleep_func,
            )
        except Exception:
            logger.exception("Recovery failed outright for %s", label)
            return label, None, gaps_by_label[label]

        if len(retry_gaps) < before:
            logger.info(
                "Recovered %s: %s gap(s) -> %s, %s rows",
                label, before, len(retry_gaps), len(rows),
            )
            return label, rows, retry_gaps

        # No improvement. Keep the first pass's rows and its gaps so a worse
        # retry can never shrink what we already had.
        logger.warning(
            "Recovery did not improve %s (%s gap(s) before, %s after); "
            "keeping the first pass.",
            label, before, len(retry_gaps),
        )
        return label, None, gaps_by_label[label]

    if workers == 1:
        results = [recover_one(label) for label in labels]
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            results = list(pool.map(recover_one, labels))

    recovered_rows: dict[str, list[dict]] = {}
    remaining: list[Gap] = []
    for label, rows, keep_gaps in results:
        if rows is not None:
            recovered_rows[label] = rows
        remaining.extend(keep_gaps)

    return recovered_rows, remaining


def write_rows(rows: list[dict], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    logger.info("Wrote %s rows to %s", len(rows), output_path)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, help="Calendar year to fetch (default: current).")
    parser.add_argument("--states", help="Comma-separated state codes, e.g. MH,GJ. Default: all.")
    parser.add_argument(
        "--limit-rtos", type=int, help="Stop after N RTOs. For smoke tests only."
    )
    parser.add_argument(
        "--sleep-seconds", type=float, default=DEFAULT_SLEEP_SECONDS,
        help=f"Pause between API calls (default {DEFAULT_SLEEP_SECONDS}).",
    )
    parser.add_argument(
        "--status-scopes",
        default=",".join(DEFAULT_STATUS_SCOPES),
        help=(
            "Comma-separated archive-status scopes to pull "
            f"(choose from {', '.join(STATUS_SCOPES)}; "
            f"default {','.join(DEFAULT_STATUS_SCOPES)}). Each extra scope "
            "roughly doubles the run."
        ),
    )
    parser.add_argument(
        "--workers", type=int, default=DEFAULT_WORKERS,
        help=(
            f"RTOs to fetch concurrently (default {DEFAULT_WORKERS}; 1 = serial). "
            "Throughput flattens past ~8 on the measured portal."
        ),
    )
    parser.add_argument("--output-dir", type=Path, help="Where to write the CSV.")
    parser.add_argument(
        "--no-recovery",
        action="store_true",
        help="Skip the second pass over offices that came back with gaps.",
    )
    parser.add_argument(
        "--recovery-wait",
        type=float,
        default=RECOVERY_SETTLE_SECONDS,
        help=(
            "Seconds to wait before the recovery pass "
            f"(default: {RECOVERY_SETTLE_SECONDS:g}). The portal's intermittent "
            "500s clear over minutes, so retrying sooner mostly re-hits them."
        ),
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help=(
            "Ignore any checkpoint and re-fetch every office. Without this, a "
            "run resumes: offices already collected without gaps are skipped, "
            "and gapped ones get another attempt."
        ),
    )
    parser.add_argument(
        "--retry-attempts",
        type=int,
        default=DEFAULT_RETRY_ATTEMPTS,
        help=(
            f"In-line attempts per portal call (default {DEFAULT_RETRY_ATTEMPTS}). "
            "Kept at 1 on purpose: retrying within seconds measurably does not "
            "work on this portal, and the recovery pass retries properly later."
        ),
    )
    parser.add_argument(
        "--allow-gaps",
        action="store_true",
        help=(
            "Exit 0 even if gaps remain after recovery. Off by default: a "
            "missing row is indistinguishable from a real zero once ingested."
        ),
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    if args.year:
        year = args.year
    else:
        # Imported here rather than at module scope so the fetch logic above
        # stays free of wall-clock dependencies and is straightforward to test.
        from datetime import date

        year = date.today().year

    states = [s.strip().upper() for s in args.states.split(",")] if args.states else None
    if states:
        known = set(STATE_NAME_TO_NEW_PORTAL_CODE.values())
        unknown = [s for s in states if s not in known]
        if unknown:
            parser.error(f"unknown state code(s): {', '.join(unknown)}")

    status_scopes = [s.strip().upper() for s in args.status_scopes.split(",") if s.strip()]
    unknown_scopes = [s for s in status_scopes if s not in STATUS_SCOPES]
    if unknown_scopes:
        parser.error(f"unknown status scope(s): {', '.join(unknown_scopes)}")
    if not status_scopes:
        parser.error("--status-scopes cannot be empty")
    if status_scopes != DEFAULT_STATUS_SCOPES:
        logger.warning(
            "Pulling non-default status scopes %s. Note that ACTIVE is an "
            "as-of-today measure, not a fixed property of the month.",
            status_scopes,
        )

    targets = load_rto_targets(states=states)
    if args.limit_rtos:
        targets = targets[: args.limit_rtos]
        logger.warning(
            "--limit-rtos=%s: this is a partial run and must NOT be ingested over "
            "a full month, since ingestion replaces the whole year's snapshot.",
            args.limit_rtos,
        )

    vehicle_class_dimensions = load_vehicle_class_dimensions()

    if args.workers < 1:
        parser.error("--workers must be at least 1")

    # Resume. The portal swings between serving and dead on a timescale of
    # minutes (2026-09-23: three runs reached 26, 133 and 320 offices before
    # conditions collapsed), so a run that must finish in one clean window may
    # never finish at all. Checkpointing each office turns an abort into a
    # resume point, and the dataset accumulates across short good windows.
    progress_path = build_progress_path(year, args.output_dir)
    if args.refresh and progress_path.exists():
        progress_path.unlink()
        logger.info("--refresh: discarded the previous checkpoint at %s", progress_path)

    progress = load_progress(progress_path)
    progress_lock = threading.Lock()

    def checkpoint(rto_label: str, rows: list[dict], gaps: list[Gap]) -> None:
        with progress_lock:
            append_progress(progress_path, rto_label, rows, gaps)

    def label_of(rto: dict) -> str:
        return f"{rto['state_code']}/{rto['new_rto_code']} {rto['new_rto_name']}"

    # Offices already finished cleanly are skipped; anything gapped is
    # re-attempted, which folds resume and recovery into one mechanism.
    done = {
        label for label, entry in progress.items() if office_is_complete(entry)
    }
    pending = order_pending(targets, progress, done, label_of)
    if progress:
        never_tried = sum(1 for rto in targets if label_of(rto) not in progress)
        logger.info(
            "Resuming from %s: %s complete, %s never attempted, %s gapped. "
            "Unvisited offices go first. Pass --refresh to start clean.",
            progress_path, len(done), never_tried, len(pending) - never_tried,
        )

    # One session per worker thread; see DEFAULT_WORKERS above for why sharing a
    # single session is nearly useless. Well under the portal's ~300 limit on
    # session creation.
    thread_state = threading.local()
    sessions_created: list[NewPortalClient] = []
    sessions_lock = threading.Lock()

    def new_client() -> NewPortalClient:
        client = NewPortalClient(retry_attempts=args.retry_attempts)
        client.session.mount("https://", HTTPAdapter(pool_connections=2, pool_maxsize=2))
        client.start_session()
        return client

    def portal_for_thread() -> NewPortalClient:
        client = getattr(thread_state, "client", None)
        if client is None:
            client = new_client()
            thread_state.client = client
            with sessions_lock:
                sessions_created.append(client)
        return client

    # Pre-flight: prove the portal is reachable with ONE session before spawning
    # a fleet of them. During the 2026-08-20 outage this is the check that turns
    # a 400-request pile-up into a single failed request and a clear message.
    try:
        preflight_client = new_client()
    except Exception as exc:
        raise SystemExit(
            f"Cannot reach the new portal ({exc}). Nothing was fetched. "
            "Check https://analytics.parivahan.gov.in/analytics/ — a 503 with "
            "'No server is available to handle this request' is a portal-side "
            "outage, not a problem with this pipeline."
        )
    thread_state.client = preflight_client
    sessions_created.append(preflight_client)

    failures: list[str] = []
    failures_lock = threading.Lock()
    completed = itertools.count(1)
    recent = collections.deque(maxlen=ABORT_WINDOW)
    aborted = threading.Event()

    def _check_breaker():
        """Caller must hold failures_lock."""
        if len(recent) >= ABORT_MIN_SAMPLE and should_abort(recent):
            aborted.set()

    def record_failure(label):
        with failures_lock:
            failures.append(label)
            recent.append(True)
            _check_breaker()

    def record_success():
        with failures_lock:
            recent.append(False)
            _check_breaker()

    def fetch_one(indexed):
        index, rto = indexed
        label = f"{rto['state_code']}/{rto['new_rto_code']} {rto['new_rto_name']}"
        if aborted.is_set():
            # None, not ([], []) — an office we never attempted must not look
            # like one that reported nothing, or the merge below records it as
            # complete and a later resume skips it entirely.
            return index, None, None
        try:
            rows, gaps = fetch_rto_year(
                portal_for_thread(),
                rto,
                year,
                vehicle_class_dimensions,
                status_scopes=status_scopes,
                sleep_seconds=args.sleep_seconds,
            )
        except Exception:
            # One unreachable office must not throw away the other 1,675. The
            # run still fails loudly at the end so a partial pull is never
            # silently ingested as if it were complete.
            logger.exception("Failed fetching %s; continuing", label)
            record_failure(label)
            if aborted.is_set():
                logger.error(
                    "Over %.0f%% of the last %s offices failed; aborting the run "
                    "rather than continuing to hammer the portal.",
                    ABORT_FAILURE_RATE * 100, ABORT_WINDOW,
                )
            # None for the same reason as the abort path: an office that blew up
            # is not an office that reported nothing, and must be re-attempted.
            return index, None, None

        # Partial success is success for the circuit breaker: the office
        # answered, so the portal is not down, and the recovery pass owns the
        # gaps. But an office that yielded *nothing* is indistinguishable from
        # the old hard-failure case, and must still count — otherwise gap
        # isolation silently disables the breaker exactly when a degraded portal
        # makes it matter most (measured 2026-09-23: 67% of probes returning
        # 500, which would otherwise grind through all 1,676 offices and a
        # sequential recovery pass before anyone noticed).
        if office_counts_as_failure(rows, gaps):
            record_failure(label)
            if aborted.is_set():
                logger.error(
                    "Over %.0f%% of the last %s offices failed every call; "
                    "aborting rather than continuing against a portal that is "
                    "not serving data.",
                    ABORT_FAILURE_RATE * 100, ABORT_WINDOW,
                )
        else:
            record_success()
        checkpoint(label, rows, gaps)
        logger.info(
            "[%s/%s] %s -> %s rows%s",
            next(completed),
            len(pending),
            label,
            len(rows),
            f" ({len(gaps)} gap(s))" if gaps else "",
        )
        return index, rows, gaps

    indexed_targets = list(enumerate(pending))
    if args.workers == 1:
        results = [fetch_one(item) for item in indexed_targets]
    else:
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            results = list(pool.map(fetch_one, indexed_targets))

    logger.info(
        "Fetched with %s worker(s) using %s session(s)",
        args.workers, len(sessions_created),
    )

    # Merge this run's results over whatever the checkpoint already held. An
    # office skipped as complete keeps its stored rows; an office re-attempted
    # keeps the better of the two (see is_better_result), so a bad window can
    # never shrink what a good one already collected.
    for index, rows, rto_gaps in results:
        if rows is None:
            # Never attempted (run already aborted) or crashed outright. Leave
            # whatever the checkpoint held so the office is retried next time.
            continue
        label = label_of(pending[index])
        if is_better_result(rows, rto_gaps, progress.get(label)):
            progress[label] = (rows, rto_gaps)

    by_label = {label_of(rto): rto for rto in targets}
    rows_by_index: dict[int, list[dict]] = {}
    label_by_index: dict[int, str] = {}
    gaps: list[Gap] = []
    for index, rto in enumerate(targets):
        label = label_of(rto)
        stored_rows, stored_gaps = progress.get(label, ([], []))
        rows_by_index[index] = stored_rows
        label_by_index[index] = label
        gaps.extend(stored_gaps)

    # Recovery pass, unless the run already bailed out — retrying into a portal
    # that just failed 25 offices in a row would be pointless.
    if gaps and not aborted.is_set() and not args.no_recovery:
        recovered, gaps = recover_gaps(
            gaps,
            by_label,
            year,
            vehicle_class_dimensions,
            client_factory=new_client,
            status_scopes=status_scopes,
            sleep_seconds=args.sleep_seconds,
            settle_seconds=args.recovery_wait,
            workers=args.workers,
        )
        # Recovered offices are checkpointed too, so their work survives a later
        # abort exactly as the main pass's does.
        remaining_by_label: dict[str, list[Gap]] = {}
        for gap in gaps:
            remaining_by_label.setdefault(gap.rto_label, []).append(gap)
        for index, label in label_by_index.items():
            if label in recovered:
                rows_by_index[index] = recovered[label]
                checkpoint(label, recovered[label], remaining_by_label.get(label, []))

    # Sort by the original target order so the CSV is byte-identical regardless
    # of how many workers produced it, and diffable between runs.
    all_rows: list[dict] = []
    for index in sorted(rows_by_index):
        all_rows.extend(rows_by_index[index])

    write_rows(all_rows, build_output_path(year, args.output_dir))

    manifest_path = build_gap_manifest_path(year, args.output_dir)
    if gaps:
        write_gap_manifest(gaps, year, manifest_path)
        logger.warning("Wrote %s gap(s) to %s", len(gaps), manifest_path)
    elif manifest_path.exists():
        # A stale manifest from an earlier run would misreport a clean pull.
        manifest_path.unlink()

    complete = sum(
        1
        for rto in targets
        if label_of(rto) in progress and office_is_complete(progress[label_of(rto)])
    )
    resume_hint = (
        f"Progress is checkpointed at {progress_path} — {complete} of "
        f"{len(targets)} office(s) are complete. Re-running resumes from there "
        f"rather than starting over."
    )

    if aborted.is_set():
        raise SystemExit(
            f"ABORTED: over {ABORT_FAILURE_RATE:.0%} of the last {ABORT_WINDOW} "
            f"offices failed ({len(failures)} failures this run). The CSV is "
            f"incomplete and must not be ingested. {resume_hint}"
        )

    if failures:
        raise SystemExit(
            f"{len(failures)} of {len(pending)} attempted RTOs failed to fetch "
            f"(e.g. {', '.join(failures[:5])}). The CSV holds only the RTOs that "
            f"succeeded — do not ingest it as a complete snapshot. {resume_hint}"
        )

    if gaps:
        affected = sorted({g.rto_label for g in gaps})
        summary = "; ".join(g.describe() for g in gaps[:5])
        message = (
            f"{len(gaps)} gap(s) remain after the recovery pass, across "
            f"{len(affected)} office(s): {summary}"
            f"{' ...' if len(gaps) > 5 else ''}. Full list: {manifest_path}. "
            f"These offices under-count, and a missing row is indistinguishable "
            f"from a real zero once ingested."
        )
        if args.allow_gaps:
            logger.warning("%s Proceeding anyway because --allow-gaps was set.", message)
        else:
            raise SystemExit(
                f"{message} {resume_hint} Re-run when the portal is healthier, "
                f"or pass --allow-gaps to ingest this pull knowing it is incomplete."
            )

    # Every office finished cleanly. Drop the checkpoint so a later run for this
    # year starts fresh instead of resuming stale state.
    if progress_path.exists():
        progress_path.unlink()
        logger.info("Run complete with no gaps; removed %s", progress_path)


if __name__ == "__main__":
    main()
