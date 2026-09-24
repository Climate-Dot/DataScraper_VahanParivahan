"""Loads the CSV produced by `new_portal/rto_fetch.py` into SQL Server.

Reuses the shared `BaseSqlServerIngestor` (staging truncate -> bulk insert ->
scoped delete from final -> transfer) unchanged; only the table names and the
replacement scope differ from V1.

Replacement scope is `["year", "status_scope", "state_code"]`, not V1's
`["date"]`, because a V2 fetch pulls a whole year at a time — so the run's output
*is* the complete snapshot for the years and states it covers, and those rows
should be replaced wholesale. This is the same reasoning behind V1's own
orphaned-row fix (commit 865fd039): scope the delete to the snapshot the run
actually replaces, so an office that legitimately reported nothing this run does
not leave a stale row behind.

`state_code` is in the scope for the same reason Telangana's historical backfill
scopes to `["date", "state"]`: the delete matches staging rows via EXISTS, so
including `state_code` confines it to the states actually present in this run.
Without it, a `--states MH` fetch would delete every other state's rows for that
year. A nationwide run is unaffected, since staging then holds every state.

The residual gap this leaves is a whole state vanishing from the portal, whose
rows would then never be deleted — far less likely than a single RTO reporting
nothing, which the scope does handle.

Even so, a partial run should not be ingested — `rto_fetch.py` exits non-zero and
warns when it produced a partial CSV, and the shell entrypoint stops there.

Usage:
    python3 -m new_portal.rto_ingest --year 2026
"""

from __future__ import annotations

import argparse
import logging
import sys

from etl_ingestion import BaseSqlServerIngestor
from pipeline_logging import configure_pipeline_logging

from new_portal.rto_fetch import FILE_PREFIX, build_output_path

logger = logging.getLogger(__name__)

FINAL_TABLE = "fact_ev_data_by_rto_v2"
STAGING_TABLE = "staging_fact_ev_data_by_rto_v2"

REPLACEMENT_SCOPE_COLUMNS = ["year", "status_scope", "state_code"]


class NewPortalRtoIngest(BaseSqlServerIngestor):
    def __init__(self):
        super().__init__(
            file_prefix=FILE_PREFIX,
            staging_table_name=STAGING_TABLE,
            final_table_name=FINAL_TABLE,
            merge_key_columns=[
                "date",
                "state_code",
                "rto_code",
                "vehicle_class",
                "status_scope",
            ],
            missing_file_hint="new_portal.rto_fetch",
            # A nationwide year is ~144k rows across 50 columns. On the default
            # executemany path that is 4.6 hours of per-row round trips against
            # an idle database; multi-row statements bring it to ~33 minutes.
            # See etl_ingestion.MAX_STATEMENT_PARAMETERS for the measurements.
            use_multirow_insert=True,
        )

    def ingest_year(self, year: int) -> int:
        return self.data_ingest_from_file(
            build_output_path(year),
            replacement_scope_columns=REPLACEMENT_SCOPE_COLUMNS,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, required=True, help="Calendar year to ingest.")
    args = parser.parse_args()

    configure_pipeline_logging()

    row_count = NewPortalRtoIngest().ingest_year(args.year)
    logger.info("Ingested %s rows into %s for %s", row_count, FINAL_TABLE, args.year)
    if row_count == 0:
        # An empty CSV means the fetch produced nothing; ingesting it silently
        # would look like a clean run that quietly changed nothing.
        sys.exit(f"No rows ingested for {args.year}; did new_portal.rto_fetch run?")


if __name__ == "__main__":
    main()
