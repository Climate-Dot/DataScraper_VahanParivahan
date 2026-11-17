#!/usr/bin/env python3
"""Clear the V2 landing and staging tables after a successful run.

**This is not an ingestion-correctness fix, and should not be read as one.**
Verified in code: ``new_portal/rto_bulk_ingest.py`` truncates the landing table
before its ``BULK INSERT`` and the staging table before the staging transfer,
and the row-by-row path in ``etl_ingestion.py`` likewise truncates staging
before loading. Both paths are self-protecting, so a monthly run does not
inherit stale rows into the fact table. Nothing is being double-counted.

What is genuinely wrong is that nothing truncates at the *end*, so both tables
permanently retain a full copy of whichever year was loaded last. Measured
2026-11-03, both held 160,573 rows of 2013 data from an old backfill. Two real
consequences:

1. **It is a loaded gun pointed at this pipeline's own rollback procedures.**
   ``rto_bulk_ingest.build_delete_query()`` deletes from the fact table where a
   matching row exists *in staging*, scoped on ``(year, status_scope,
   state_code)``. In the normal flow staging was just repopulated, so that is
   correct. But recovery is a manual session, and a human or an agent who runs
   that delete -- or ``INSERT INTO final SELECT * FROM staging`` -- by hand
   against the current contents would operate on **2013**, deleting and
   re-inserting a year nobody intended to touch. That is the exact shape of the
   orphaned-row and duplicate-data incidents in the August 2026 remediation.
2. **It misleads anyone inspecting state.** Querying staging to see what the
   last run loaded returns 2013, which reads as a far worse problem than it is
   and costs an investigation.

A ``TRUNCATE`` is not recoverable, so the safety argument has to hold before the
fact. It does: both tables are rebuilt from scratch by the next ingest, the only
data they hold is a copy of a year that already exists in the fact table and in
an immutable blob snapshot, and nothing reads them between runs. Take no backup
of these two -- backing up scratch state would institutionalise the confusion
this script exists to remove.

The table list is derived from the ingest modules rather than re-spelled, and
anything outside it is refused rather than truncated.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from typing import Iterable

try:
    import pyodbc
except ImportError:  # pragma: no cover - exercised by a test that patches it
    pyodbc = None

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.append(REPO_ROOT)

from new_portal.rto_bulk_ingest import LANDING_TABLE
from new_portal.rto_ingest import STAGING_TABLE
from runtime_config import load_config
from sqlserver_utils import connect_with_retry

logger = logging.getLogger(__name__)

# Derived, never re-spelled: if either ingest module renames its table, this
# follows. The fact table is deliberately absent and may never be added.
SCRATCH_TABLES: tuple[str, ...] = (LANDING_TABLE, STAGING_TABLE)

_SAFE_TABLE = re.compile(r"^[A-Za-z0-9_]+$")


def _safe_table(table_name: str) -> str:
    if not _SAFE_TABLE.match(table_name):
        raise ValueError(f"Unsafe table name: {table_name!r}")
    return table_name


def connect():
    """Open a connection to the configured SQL Server."""
    if pyodbc is None:
        raise RuntimeError(
            "pyodbc is not installed; this script writes to the database and "
            "must run on the VM inside its venv."
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


def truncate_scratch_tables(
    connection,
    *,
    tables: Iterable[str] = SCRATCH_TABLES,
    dry_run: bool = False,
) -> dict[str, int]:
    """Empty the scratch tables, returning the row count each one held.

    Counting before truncating is what makes the log say what was cleared; a
    ``TRUNCATE`` reports nothing of its own.
    """
    tables = tuple(tables)
    outside = [table for table in tables if table not in SCRATCH_TABLES]
    if outside:
        raise ValueError(
            f"Refusing to truncate tables outside the scratch list: {outside}. "
            f"Only {list(SCRATCH_TABLES)} may ever be truncated here."
        )

    removed: dict[str, int] = {}
    cursor = connection.cursor()
    for table in tables:
        safe = _safe_table(table)
        cursor.execute(f"SELECT COUNT(*) FROM dbo.{safe}")
        row = cursor.fetchone()
        count = int(row[0]) if row and row[0] is not None else 0
        removed[table] = count
        if dry_run:
            logger.info("[dry-run] would truncate dbo.%s (%s rows)", safe, count)
            continue
        cursor.execute(f"TRUNCATE TABLE dbo.{safe}")
        logger.info("Truncated dbo.%s (%s rows removed)", safe, count)

    if not dry_run:
        connection.commit()
    return removed


def main(argv: list[str] | None = None) -> int:
    """Exit non-zero only on a database error; an empty table is success."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Count and log what would be cleared, truncating nothing.",
    )
    args = parser.parse_args(argv)

    connection = connect()
    try:
        removed = truncate_scratch_tables(connection, dry_run=args.dry_run)
    finally:
        connection.close()

    logger.info(
        "Scratch cleanup %s: %s",
        "dry run" if args.dry_run else "complete",
        ", ".join(f"{table}={count}" for table, count in removed.items()),
    )
    return 0


if __name__ == "__main__":
    from pipeline_logging import configure_pipeline_logging

    configure_pipeline_logging()
    sys.exit(main())
