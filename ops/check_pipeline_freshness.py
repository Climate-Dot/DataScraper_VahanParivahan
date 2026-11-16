"""Alert when a pipeline has stopped loading, or loaded an unsettled month.

Two independent predicates, because two different things go wrong and the
operator response differs:

1. **Load recency** -- did a run land at all? This is the staleness signal, and
   it keys on ``MAX([inserted_at])``.
2. **Month settled** -- did the month we came for actually fill up, or is it
   another in-flight stub?

### Why ``MAX([date])`` cannot be the staleness signal

A V2 run pulls a whole year, so every run writes rows for the in-flight current
month. A December 3 run leaves ``MAX([date])`` = December. If the January 3 run
then fails completely, ``MAX([date])`` is *still* December, "previous calendar
month" is *also* December, a ``MAX([date])``-based check compares them equal and
reports FRESH -- while the newest month in the table is a 3-day stub and the
pipeline has been dead for a month.

That is not hypothetical: it is what the table looked like through October 2026.
``MAX([date])`` was September 2026, captured 2026-09-24 with six days of the
month still unelapsed, and no October rows existed at all. A ``MAX([date])``
check would have reported healthy throughout. ``MAX([inserted_at])`` answers
"did a run land recently", which is the actual question.

### Why the month-settled check uses ``SUM([total])`` and not ``COUNT(*)``

A partial-month capture keeps full *breadth* -- every state, every office
reporting -- and shrinks only the counts inside each row. ``COUNT(*)`` measures
breadth, which is exactly what a stub does not lose. Measured on 2026,
``ALL_STATUSES``: healthy months sit at 0.9818-1.0210 month-over-month on row
count and the September stub sits at 0.9255, above any usable threshold. On
``SUM([total])`` healthy is 0.8878-1.1162 and the stub is 0.7763, which
separates. The EV-only sum also separates but false-alarms, because its healthy
range reaches down to 0.8615.

``COUNT(*)`` stays in the query and in the alert body as reported context only,
labelled as such so nobody later promotes it to a threshold.

Guarded ``pyodbc`` import so the module stays importable under the CI-only
requirements; the alert goes out through ``ops/send_chat_alert.py`` as a
subprocess, exactly as ``ops/etl_runtime.sh`` does it, so the alert path is the
one production already exercises.
"""

from __future__ import annotations

import argparse
import dataclasses
import logging
import os
import re
import socket
import subprocess
import sys
from datetime import date, datetime

try:
    import pyodbc
except ImportError:  # pragma: no cover - exercised by a test that patches it
    pyodbc = None

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.append(REPO_ROOT)

from runtime_config import load_config
from sqlserver_utils import connect_with_retry

logger = logging.getLogger(__name__)

SEND_CHAT_ALERT = os.path.join(REPO_ROOT, "ops", "send_chat_alert.py")

# PRIMARY: did a run land at all? Never MAX([date]) -- see the module docstring.
LAST_LOADED_AT_QUERY = """
SELECT MAX([inserted_at]) AS last_loaded_at
FROM dbo.{table}
WHERE [status_scope] = ?
"""

# SECONDARY: did the target month settle, or is it another in-flight stub?
# Grouping on [year], [month] keeps the December-to-January boundary a parameter
# concern rather than a SQL one.
MONTH_STATS_QUERY = """
SELECT [year], [month], COUNT(*) AS row_count,
       SUM(CAST([total] AS BIGINT)) AS total_registrations
FROM dbo.{table}
WHERE [status_scope] = ? AND [year] = ? AND [month] IN ({placeholders})
GROUP BY [year], [month]
ORDER BY [year], [month]
"""

_SAFE_TABLE = re.compile(r"^[A-Za-z0-9_]+$")


@dataclasses.dataclass(frozen=True)
class MonthStats:
    """One month's breadth and magnitude."""

    row_count: int
    total_registrations: int | None


@dataclasses.dataclass(frozen=True)
class FreshnessVerdict:
    is_stale: bool
    age_days: float | None
    message: str


def _now() -> datetime:
    """Seam so tests can pin the reference instant."""
    return datetime.now()


def _safe_table(table_name: str) -> str:
    """A table name cannot be a bound parameter, so validate it instead."""
    if not _SAFE_TABLE.match(table_name):
        raise ValueError(f"Unsafe table name: {table_name!r}")
    return table_name


def target_month(reference_date: date) -> tuple[int, int]:
    """The previous calendar month, which is always fully elapsed.

    No grace period: the target is a closed month, so there is nothing to wait
    for.
    """
    if reference_date.month == 1:
        return reference_date.year - 1, 12
    return reference_date.year, reference_date.month - 1


def preceding_month(year: int, month: int) -> tuple[int, int]:
    """The month before a given month, as the known-settled baseline."""
    if month == 1:
        return year - 1, 12
    return year, month - 1


def connect():
    """Open a connection to the configured SQL Server."""
    if pyodbc is None:
        raise RuntimeError(
            "pyodbc is not installed; this check reads the database and must "
            "run on the VM inside its venv."
        )
    config = load_config()
    database = config["database"]
    return connect_with_retry(
        lambda: pyodbc.connect(
            driver="{ODBC Driver 18 for SQL Server}",
            server=database["server"],
            database=database["database"],
            uid=database["username"],
            pwd=database["password"],
            timeout=30,
        ),
        logger=logger,
        connection_error_types=(pyodbc.Error,),
    )


def fetch_last_loaded_at(
    connection, table_name: str, *, status_scope: str
) -> datetime | None:
    """``MAX([inserted_at])`` for a status scope, or ``None`` if nothing loaded."""
    cursor = connection.cursor()
    cursor.execute(
        LAST_LOADED_AT_QUERY.format(table=_safe_table(table_name)), (status_scope,)
    )
    row = cursor.fetchone()
    return row[0] if row else None


def fetch_month_row_counts(
    connection,
    table_name: str,
    *,
    status_scope: str,
    year: int,
    months: tuple[int, ...],
) -> dict[int, MonthStats]:
    """Row count and total registrations per month, keyed by month number."""
    placeholders = ", ".join("?" for _ in months)
    cursor = connection.cursor()
    cursor.execute(
        MONTH_STATS_QUERY.format(
            table=_safe_table(table_name), placeholders=placeholders
        ),
        (status_scope, year, *months),
    )
    stats: dict[int, MonthStats] = {}
    for row in cursor.fetchall():
        total = row[3]
        stats[int(row[1])] = MonthStats(
            row_count=int(row[2]),
            total_registrations=None if total is None else int(total),
        )
    return stats


def fetch_month_stats_pair(
    connection,
    table_name: str,
    *,
    status_scope: str,
    target: tuple[int, int],
    preceding: tuple[int, int],
) -> tuple[MonthStats | None, MonthStats | None]:
    """Stats for the target month and its baseline, across a year boundary.

    A January target's baseline is the previous December, so the two months can
    fall in different years and a single ``WHERE [year] = ?`` cannot fetch both.
    """
    target_year, target_number = target
    preceding_year, preceding_number = preceding

    if target_year == preceding_year:
        stats = fetch_month_row_counts(
            connection,
            table_name,
            status_scope=status_scope,
            year=target_year,
            months=(preceding_number, target_number),
        )
        return stats.get(target_number), stats.get(preceding_number)

    target_stats = fetch_month_row_counts(
        connection,
        table_name,
        status_scope=status_scope,
        year=target_year,
        months=(target_number,),
    )
    preceding_stats = fetch_month_row_counts(
        connection,
        table_name,
        status_scope=status_scope,
        year=preceding_year,
        months=(preceding_number,),
    )
    return target_stats.get(target_number), preceding_stats.get(preceding_number)


def evaluate_load_recency(
    last_loaded_at: datetime | None,
    reference: datetime,
    *,
    max_load_age_days: int,
) -> FreshnessVerdict:
    """Whether a run has landed recently enough."""
    if last_loaded_at is None:
        return FreshnessVerdict(
            is_stale=True,
            age_days=None,
            message=(
                "no run has ever loaded this table for this status scope"
            ),
        )

    age_days = (reference - last_loaded_at).total_seconds() / 86400
    if age_days > max_load_age_days:
        return FreshnessVerdict(
            is_stale=True,
            age_days=age_days,
            message=(
                f"last load was {age_days:.1f} days ago "
                f"({last_loaded_at.isoformat(sep=' ')}), over the "
                f"{max_load_age_days}-day limit"
            ),
        )
    return FreshnessVerdict(
        is_stale=False,
        age_days=age_days,
        message=(
            f"last load {age_days:.1f} days ago "
            f"({last_loaded_at.isoformat(sep=' ')}), limit {max_load_age_days}"
        ),
    )


def evaluate_month_settled(
    target: MonthStats | None,
    preceding: MonthStats | None,
    *,
    min_month_total_ratio: float,
) -> FreshnessVerdict:
    """Whether the target month filled up, judged on registrations not rows."""
    if target is None:
        return FreshnessVerdict(
            is_stale=True,
            age_days=None,
            message="no rows at all for the target month",
        )
    if preceding is None or preceding.total_registrations in (None, 0):
        return FreshnessVerdict(
            is_stale=False,
            age_days=None,
            message=(
                "no preceding month to compare against, so the month-settled "
                "check is not applicable"
            ),
        )
    if target.total_registrations is None:
        return FreshnessVerdict(
            is_stale=True,
            age_days=None,
            message="target month has no registration total to compare",
        )

    total_ratio = target.total_registrations / preceding.total_registrations
    row_ratio = (
        target.row_count / preceding.row_count if preceding.row_count else float("nan")
    )
    detail = (
        f"total {target.total_registrations:,} vs {preceding.total_registrations:,} "
        f"(ratio {total_ratio:.4f}); rows {target.row_count:,} vs "
        f"{preceding.row_count:,} (ratio {row_ratio:.4f}, reported as context "
        f"only -- row count measures breadth, which a partial capture does not "
        f"lose, so it must never be a pass/fail signal)"
    )
    if total_ratio < min_month_total_ratio:
        return FreshnessVerdict(
            is_stale=True,
            age_days=None,
            message=f"{detail}; below the {min_month_total_ratio} floor",
        )
    return FreshnessVerdict(is_stale=False, age_days=None, message=detail)


def send_failure_alert(*, pipeline: str, details: str) -> int:
    """Post one FAILED alert through the same script the ETL runtime uses."""
    command = [
        sys.executable,
        SEND_CHAT_ALERT,
        "--pipeline",
        pipeline,
        "--status",
        "FAILED",
        "--step",
        "freshness_check",
        "--hostname",
        socket.gethostname(),
        "--details",
        details,
    ]
    completed = subprocess.run(command, check=False)
    return getattr(completed, "returncode", 0)


def main(argv: list[str] | None = None) -> int:
    """Exit 1 and send one alert if either check fails; exit 0 and stay silent."""
    parser = argparse.ArgumentParser(description="Pipeline freshness check.")
    parser.add_argument("--table", required=True)
    parser.add_argument("--status-scope", required=True)
    parser.add_argument(
        "--max-load-age-days",
        type=int,
        required=True,
        help="Derived from the schedule's retry day; see the runbook.",
    )
    parser.add_argument("--min-month-total-ratio", type=float, required=True)
    parser.add_argument("--pipeline", required=True)
    args = parser.parse_args(argv)

    reference = _now()
    target = target_month(reference.date())
    preceding = preceding_month(*target)

    connection = connect()
    try:
        last_loaded_at = fetch_last_loaded_at(
            connection, args.table, status_scope=args.status_scope
        )
        target_stats, preceding_stats = fetch_month_stats_pair(
            connection,
            args.table,
            status_scope=args.status_scope,
            target=target,
            preceding=preceding,
        )
    finally:
        connection.close()

    recency = evaluate_load_recency(
        last_loaded_at, reference, max_load_age_days=args.max_load_age_days
    )
    settled = evaluate_month_settled(
        target_stats, preceding_stats, min_month_total_ratio=args.min_month_total_ratio
    )

    lines = [f"table {args.table}, status scope {args.status_scope}"]
    if recency.is_stale:
        lines.append(
            f"LOAD RECENCY is stale: {recency.message}. No run has landed -- the "
            f"VM was up to run this check, so investigate the schedule."
        )
    else:
        lines.append(f"load recency ok: {recency.message}")

    if settled.is_stale:
        lines.append(
            f"MONTH SETTLED failed for {target[0]}-{target[1]:02d}: "
            f"{settled.message}. A run landed but captured an in-flight month "
            f"-- re-run now that the month is closed."
        )
    else:
        lines.append(f"month settled ok: {settled.message}")

    details = "\n".join(lines)

    if not (recency.is_stale or settled.is_stale):
        logger.info("Freshness check passed. %s", details.replace("\n", " | "))
        return 0

    logger.error("Freshness check failed. %s", details.replace("\n", " | "))
    send_failure_alert(pipeline=args.pipeline, details=details)
    return 1


if __name__ == "__main__":
    from pipeline_logging import configure_pipeline_logging

    configure_pipeline_logging()
    sys.exit(main())
