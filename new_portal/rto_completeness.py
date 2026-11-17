"""Decide whether one year's fetch output is safe to ingest.

The sweep's exit code says "this invocation finished with no gaps". It does not
say that the CSV sitting on disk is that invocation's output, that the run
covered the whole country, or that it covers the month we are scheduling for.
Four holes it leaves, all closed here:

- The CSV path for a year is fixed, so a stale CSV from an earlier failed run
  sits at exactly the path the snapshot and ingest stages read. Nothing else
  proves provenance -> ``min_fetched_at`` against the CSV's mtime.
- A checkpoint built by a ``--states MH`` run makes "all offices complete" true
  for 48 offices -> the expected count comes from ``load_rto_targets()`` with no
  state filter.
- ``--allow-gaps`` makes ``rto_fetch`` exit 0 *with* gaps -> the gap manifest's
  mere existence is a failure.
- A sweep can complete a year that holds no rows for the month we care about ->
  ``target_month`` must appear in the CSV.

It must also not over-assert, in two directions:

- An office that genuinely reported nothing all year is legitimate and
  contributes no CSV rows (the first 13 offices in the national list are Andaman
  islands with no 2026 registrations). So office coverage is checked against the
  **checkpoint**, where an empty office is still recorded and still complete,
  and the CSV is checked only on aggregate month coverage -- never per office.
- A **successful** sweep leaves no checkpoint at all. ``rto_fetch`` deletes it
  on a clean completion ("Run complete with no gaps; removed
  ...__progress.jsonl"), so counting complete offices from that file would
  reject every good run. The on-disk signature of success is therefore: no
  checkpoint, no gap manifest, and a CSV this run wrote. An absent checkpoint is
  accepted only when all three hold -- and only when a ``min_fetched_at`` floor
  was supplied, because without one "no checkpoint" is indistinguishable from
  "no sweep ever ran here". A partial run always leaves a checkpoint, and
  ``--allow-gaps`` reaches the deleting path only after writing the manifest, so
  neither can disguise itself as the clean case. That branch still asserts a
  floor on the CSV's own breadth, so it is never a no-op: without it the gate
  would check nothing at all on the only path production takes.
- The newest month in the CSV is *expected* to be partial, because one fetch
  returns a whole year and a run on day 2 includes two days of the current
  month. So the assertion is on ``target_month``, the previous and fully
  elapsed calendar month, and never on the in-flight month.

Deliberately free of ``pyodbc``, ``azure`` and ``pandas`` so it imports under
the CI-only requirements and can be unit tested without a database.
"""

from __future__ import annotations

import argparse
import csv
import dataclasses
import logging
from datetime import datetime, timezone
from pathlib import Path

from new_portal.rto_fetch import (
    build_gap_manifest_path,
    build_output_path,
    build_progress_path,
    load_progress,
    load_rto_targets,
    office_is_complete,
)
from new_portal.schema import CSV_COLUMNS, STATUS_SCOPE_ALL

logger = logging.getLogger(__name__)

# How much of the expected office list must actually appear in the CSV before a
# checkpoint-less run is believed. Offices that reported nothing all year
# legitimately contribute no rows -- the real 2026 pull holds 1,398 of 1,676
# targets, or 0.83 -- so this cannot be set near 1. It exists to catch the
# shape of a narrow run: a single-state sweep is 48 of 1,676, or 0.03.
MIN_CSV_OFFICE_FRACTION = 0.5


@dataclasses.dataclass(frozen=True)
class CsvSummary:
    """What the CSV on disk actually contains."""

    offices: int
    rows: int
    months: set[int]
    status_scopes: set[str]


@dataclasses.dataclass
class CompletenessReport:
    """The gate's finding. ``failures`` empty means safe to ingest."""

    expected_offices: int
    complete_offices: int
    checkpoint_present: bool
    csv_offices: int
    csv_rows: int
    months_present: set[int]
    gap_manifest_present: bool
    csv_fetched_at: datetime | None
    stale_csv: bool
    failures: list[str]


def _as_datetime(value) -> datetime:
    """Accept a datetime or Unix epoch seconds; the CLI can only pass the latter."""
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return datetime.fromtimestamp(float(value), tz=timezone.utc)


def summarize_csv(csv_path: Path) -> CsvSummary:
    """Offices, rows, months and status scopes present in a fetch CSV."""
    offices: set[tuple[str, str]] = set()
    months: set[int] = set()
    scopes: set[str] = set()
    rows = 0

    with Path(csv_path).open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            rows += 1
            # rto_code is unique only within a state, so the office identity is
            # the pair. Counting bare codes collapses MH/1 and KL/1 into one.
            offices.add((row.get("state_code", ""), row.get("rto_code", "")))
            month = (row.get("month") or "").strip()
            if month:
                try:
                    months.add(int(month))
                except ValueError:
                    logger.warning("Unparseable month %r in %s", month, csv_path)
            scope = (row.get("status_scope") or "").strip()
            if scope:
                scopes.add(scope)

    return CsvSummary(
        offices=len(offices), rows=rows, months=months, status_scopes=scopes
    )


def _csv_header(csv_path: Path) -> list[str]:
    with Path(csv_path).open(newline="", encoding="utf-8") as handle:
        return next(csv.reader(handle), [])


def verify_run_complete(
    year: int,
    target_month: int,
    *,
    csv_path: Path | None = None,
    gap_manifest_path: Path | None = None,
    expected_office_count: int | None = None,
    min_fetched_at=None,
) -> CompletenessReport:
    """Check a year's fetch artifacts and report every reason not to ingest."""
    csv_path = Path(csv_path) if csv_path else build_output_path(year)
    output_dir = csv_path.parent
    gap_manifest_path = (
        Path(gap_manifest_path)
        if gap_manifest_path
        else build_gap_manifest_path(year, output_dir)
    )
    progress_path = build_progress_path(year, output_dir)

    failures: list[str] = []

    checkpoint_present = progress_path.exists()
    progress = load_progress(progress_path)

    # Count only labels that are still in the target list. The checkpoint is
    # append-only and survives a crosswalk refresh, so stale labels from
    # renamed or removed offices would otherwise inflate the count past the
    # expectation while real coverage is short -- suppressing the only coverage
    # failure this gate produces. rto_fetch's own count filters the same way.
    target_labels: set[str] | None = None
    if expected_office_count is None:
        targets = load_rto_targets()
        target_labels = {
            f"{target['state_code']}/{target['new_rto_code']} "
            f"{target['new_rto_name']}"
            for target in targets
        }
        expected_office_count = len(targets)

    complete_offices = sum(
        1
        for label, entry in progress.items()
        if office_is_complete(entry)
        and (target_labels is None or label in target_labels)
    )

    gap_manifest_present = gap_manifest_path.exists()
    if gap_manifest_present:
        failures.append(
            f"gap manifest present at {gap_manifest_path}; the fetch left gaps, "
            f"so the CSV under-counts and a missing row would read as a zero"
        )

    if not csv_path.exists():
        failures.append(f"no CSV at {csv_path}; there is nothing to ingest")
        return CompletenessReport(
            expected_offices=expected_office_count,
            complete_offices=complete_offices,
            checkpoint_present=checkpoint_present,
            csv_offices=0,
            csv_rows=0,
            months_present=set(),
            gap_manifest_present=gap_manifest_present,
            csv_fetched_at=None,
            stale_csv=False,
            failures=failures,
        )

    fetched_at = datetime.fromtimestamp(
        csv_path.stat().st_mtime, tz=timezone.utc
    )
    stale_csv = False
    if min_fetched_at is not None:
        floor = _as_datetime(min_fetched_at)
        if fetched_at < floor:
            stale_csv = True
            failures.append(
                f"CSV at {csv_path} was written {fetched_at.isoformat()}, "
                f"before this run began {floor.isoformat()}; it is a leftover "
                f"from an earlier run, not this one's output"
            )

    header = _csv_header(csv_path)
    if header != CSV_COLUMNS:
        failures.append(
            f"CSV header at {csv_path} does not match the expected schema; "
            f"ingestion maps columns by position"
        )

    summary = summarize_csv(csv_path)

    # Office coverage, decided only now that manifest presence, CSV provenance
    # and the CSV's own breadth are all known -- the clean-completion case
    # depends on all three.
    if complete_offices < expected_office_count:
        if checkpoint_present:
            failures.append(
                f"checkpoint {progress_path} has {complete_offices} complete "
                f"offices, expected {expected_office_count}; the sweep did not "
                f"cover the whole country"
            )
        elif min_fetched_at is None:
            failures.append(
                f"no checkpoint at {progress_path} and no --min-fetched-at "
                f"floor, so nothing establishes provenance: a clean sweep and "
                f"a machine where no sweep ever ran look identical"
            )
        elif not (gap_manifest_present or stale_csv):
            # The path production always takes, because a clean sweep removes
            # its own checkpoint. It must still assert something about
            # coverage, or the gate checks nothing at all on the real path and
            # rests entirely on the fetch's exit code -- the very thing it is
            # supposed to be independent of.
            minimum_offices = int(expected_office_count * MIN_CSV_OFFICE_FRACTION)
            if summary.offices < minimum_offices:
                failures.append(
                    f"CSV holds only {summary.offices} offices against "
                    f"{expected_office_count} expected, below the "
                    f"{MIN_CSV_OFFICE_FRACTION:.0%} floor of "
                    f"{minimum_offices}; this looks like a narrow run, not a "
                    f"nationwide one"
                )
            else:
                logger.info(
                    "No checkpoint at %s and no gap manifest, with a CSV newer "
                    "than this run's floor and %s of %s offices present: the "
                    "sweep completed cleanly and removed its own checkpoint.",
                    progress_path,
                    summary.offices,
                    expected_office_count,
                )
    if target_month not in summary.months:
        failures.append(
            f"CSV has no rows for target month {year}-{target_month:02d}; "
            f"months present: {sorted(summary.months)}"
        )
    unexpected_scopes = sorted(summary.status_scopes - {STATUS_SCOPE_ALL})
    if unexpected_scopes:
        failures.append(
            f"CSV carries unexpected status_scope values {unexpected_scopes}; "
            f"only {STATUS_SCOPE_ALL} may be ingested, because ACTIVE is an "
            f"as-of-today measure that decays"
        )
    if summary.rows == 0:
        failures.append(f"CSV at {csv_path} holds no data rows")

    return CompletenessReport(
        expected_offices=expected_office_count,
        complete_offices=complete_offices,
        checkpoint_present=checkpoint_present,
        csv_offices=summary.offices,
        csv_rows=summary.rows,
        months_present=summary.months,
        gap_manifest_present=gap_manifest_present,
        csv_fetched_at=fetched_at,
        stale_csv=stale_csv,
        failures=failures,
    )


def main(argv: list[str] | None = None) -> int:
    """Exit 0 only when the run is safe to ingest; log every reason it is not."""
    parser = argparse.ArgumentParser(description="V2 fetch completeness gate.")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument(
        "--target-month",
        type=int,
        required=True,
        help="The fully elapsed month the run must cover, 1-12.",
    )
    parser.add_argument(
        "--min-fetched-at",
        type=float,
        default=None,
        help="Unix epoch seconds; the CSV must be newer, proving this run wrote it.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Where the fetch artifacts live. Defaults to the repo root.",
    )
    parser.add_argument(
        "--expected-offices",
        type=int,
        default=None,
        help="Override the expected office count; defaults to the live crosswalk.",
    )
    args = parser.parse_args(argv)

    csv_path = (
        build_output_path(args.year, args.output_dir)
        if args.output_dir
        else None
    )
    report = verify_run_complete(
        args.year,
        args.target_month,
        csv_path=csv_path,
        expected_office_count=args.expected_offices,
        min_fetched_at=args.min_fetched_at,
    )

    logger.info(
        "Completeness gate year=%s target_month=%s offices=%s/%s "
        "checkpoint_present=%s csv_rows=%s csv_offices=%s months=%s",
        args.year,
        args.target_month,
        report.complete_offices,
        report.expected_offices,
        report.checkpoint_present,
        report.csv_rows,
        report.csv_offices,
        sorted(report.months_present),
    )
    if not report.failures:
        logger.info("Completeness gate passed; the run is safe to ingest.")
        return 0

    for failure in report.failures:
        logger.error("Completeness gate failure: %s", failure)
    logger.error(
        "Refusing to ingest year=%s: %s failure(s).", args.year, len(report.failures)
    )
    return 1


if __name__ == "__main__":
    import sys

    from pipeline_logging import configure_pipeline_logging

    configure_pipeline_logging()
    sys.exit(main())
