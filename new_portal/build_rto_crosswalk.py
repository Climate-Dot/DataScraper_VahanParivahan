"""Regenerates climate_dot_dbt/seeds/rto_code_crosswalk.csv, relating the new
portal's numeric rtoCode to today's alphanumeric rto_code (e.g. "HR2", "AP131").

See docs/new_portal_v2_design.md, decision 4.

The two ID schemes are NOT unrelated (an earlier version of this script and of
decision 4 wrongly assumed they were, and matched on office name instead).
Verified against all 36 states / 1,676 live RTOs on 2026-08-18:

    legacy_rto_code == legacy_state_prefix + str(new_rto_code)

with zero exceptions, and the portal's own ``rtoName`` now literally ends in
" - <STATE_CODE><rtoCode>" (e.g. "PUNE - MH12") for all 1,676 of them. So the
mapping is derived arithmetically and then *verified* against that embedded
suffix, rather than guessed at by fuzzy name matching — which is both exact and
immune to the portal renaming an office.

The one prefix alias is Odisha: the portal's state code is "OR" while today's
rto_codes use the "OD" prefix. See LEGACY_STATE_PREFIX_OVERRIDES.

The live portal list (1,676) is the driver, not V1's history (1,409), because
V2 ingests forward from cutover (decision 8) and must cover offices that never
appeared in V1. The DB is still queried, but only to attach legacy office names
and to flag legacy RTOs the portal no longer lists.

Run on the VM, where pyodbc, the real DB credentials, and network access to
the new portal are all available:
    python3 -m new_portal.build_rto_crosswalk

Without DB access the portal side alone can still be regenerated:
    python3 -m new_portal.build_rto_crosswalk --no-db
"""

from __future__ import annotations

import argparse
import csv
import logging
import re
from pathlib import Path

try:
    import pyodbc
except ImportError:  # pragma: no cover - pyodbc needs system ODBC libs absent in CI
    pyodbc = None

from pipeline_constants import STATE_NAME_TO_NEW_PORTAL_CODE
from runtime_config import load_config
from sqlserver_utils import connect_with_retry

from new_portal.client import NewPortalClient

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_PATH = REPO_ROOT / "climate_dot_dbt" / "seeds" / "rto_code_crosswalk.csv"

SOURCE_TABLE = "fact_ev_data_by_rto"

# Portal state code -> the prefix today's rto_codes actually use. Only Odisha
# differs; verified across all 36 states on 2026-08-18.
LEGACY_STATE_PREFIX_OVERRIDES = {"OR": "OD"}

# Present on both sides — the normal case.
LINK_LINKED = "linked"
# On the portal but with no V1 history: a newly listed office, or one that
# simply never registered anything V1 captured. V2 should still ingest it.
LINK_PORTAL_ONLY = "portal_only"
# In V1 history but absent from the portal's list, so V2 cannot ingest it.
LINK_LEGACY_ONLY = "legacy_only"

# rtoName suffix the portal appends, e.g. "PUNE - MH12".
_NAME_SUFFIX_PATTERN = re.compile(r"\s*-\s*([A-Z]{2}\d+)\s*$")


def connect():
    config = load_config()
    db = config["database"]
    return connect_with_retry(
        lambda: pyodbc.connect(
            driver="{ODBC Driver 18 for SQL Server}",
            server=db["server"],
            database=db["database"],
            uid=db["username"],
            pwd=db["password"],
            timeout=30,
        ),
        logger=logger,
        connection_error_types=(pyodbc.Error,),
    )


def fetch_legacy_rtos(cursor) -> list[tuple]:
    """[(state, rto_code, rto_name), ...] — exactly one row per rto_code.

    Roughly two dozen codes have two historical names on file (CLAUDE.md's
    office-rename note). The code, not the name, is what links the two sources,
    so `legacy_rto_name` is purely a human label — but it still has to be picked
    *deterministically*, or every regeneration produces a spurious diff on those
    rows. An earlier version selected `DISTINCT state, rto_code, rto_name` and
    kept whichever row arrived first, which is exactly that bug: it produced 13
    differing names between two runs over identical data.

    The name from the most recent month of data wins, matching the "canonical
    name" convention the August 2026 dedup already established, with the name
    itself as a tiebreak so the result is fully determined.

    Ordering uses `year`/`month`, not `date`. V1's `date` column is text in
    DD/MM/YYYY form, which SQL Server's default CAST silently misreads as
    MM/DD/YYYY (the curated dbt models get this right with
    `CONVERT(DATE, [date], 103)`; ad-hoc CASTs do not). `year` and `month` are
    unambiguous.
    """
    cursor.execute(
        f"""
        SELECT state, rto_code, rto_name
        FROM (
            SELECT
                state,
                rto_code,
                rto_name,
                ROW_NUMBER() OVER (
                    PARTITION BY rto_code
                    ORDER BY
                        MAX(TRY_CAST([year] AS INT) * 100 + TRY_CAST([month] AS INT)) DESC,
                        rto_name ASC
                ) AS name_rank
            FROM {SOURCE_TABLE}
            GROUP BY state, rto_code, rto_name
        ) ranked
        WHERE name_rank = 1
        ORDER BY state, rto_code
        """
    )
    return cursor.fetchall()


def legacy_state_prefix(state_code: str) -> str:
    return LEGACY_STATE_PREFIX_OVERRIDES.get(state_code, state_code)


def derive_legacy_code(state_code: str, new_rto_code) -> str:
    """"MH", 12 -> "MH12"; "OR", 1 -> "OD1"."""
    return f"{legacy_state_prefix(state_code)}{int(new_rto_code)}"


def strip_code_suffix(rto_name: str) -> str:
    """"PUNE - MH12" -> "PUNE". Leaves names without the suffix untouched."""
    return _NAME_SUFFIX_PATTERN.sub("", rto_name).strip()


def embedded_code(rto_name: str) -> str | None:
    """The "MH12" out of "PUNE - MH12", or None if the portal omits it."""
    match = _NAME_SUFFIX_PATTERN.search(rto_name)
    return match.group(1) if match else None


def verify_embedded_code(state_code: str, rto: dict) -> None:
    """Warn if the portal's own embedded code disagrees with the derived one.

    This is the guard that makes the arithmetic rule safe to rely on: if the
    portal ever renumbers, this fires instead of silently mismapping an office
    (which is exactly the class of bug that corrupted V1 — see CLAUDE.md's
    office-selector substring collision).
    """
    found = embedded_code(rto["rtoName"])
    if found is None:
        logger.warning(
            "%s rtoCode=%s (%r) has no embedded code suffix; "
            "cannot verify the derived legacy code",
            state_code,
            rto["rtoCode"],
            rto["rtoName"],
        )
        return

    expected = f"{state_code}{int(rto['rtoCode'])}"
    if found != expected:
        logger.warning(
            "%s rtoCode=%s (%r): embedded code %r != expected %r — "
            "the portal may have renumbered; review before trusting this row",
            state_code,
            rto["rtoCode"],
            rto["rtoName"],
            found,
            expected,
        )


def build_crosswalk_rows(
    legacy_rtos: list[tuple], portal: NewPortalClient
) -> list[dict]:
    legacy_names: dict[str, str] = {}
    legacy_states: dict[str, str] = {}
    for state, code, name in legacy_rtos:
        legacy_names.setdefault(code, name)
        legacy_states.setdefault(code, state)

    state_codes: dict[str, str] = {}
    for state, state_code in STATE_NAME_TO_NEW_PORTAL_CODE.items():
        state_codes.setdefault(state_code, state)

    output_rows: list[dict] = []
    seen_legacy_codes: set[str] = set()

    for state_code, state in sorted(state_codes.items()):
        for rto in portal.get_rtos_for_state(state_code):
            verify_embedded_code(state_code, rto)
            legacy_code = derive_legacy_code(state_code, rto["rtoCode"])
            seen_legacy_codes.add(legacy_code)
            has_history = legacy_code in legacy_names
            output_rows.append(
                {
                    "state": legacy_states.get(legacy_code, state),
                    "state_code": state_code,
                    "new_rto_code": int(rto["rtoCode"]),
                    "new_rto_name": strip_code_suffix(rto["rtoName"]),
                    "legacy_rto_code": legacy_code,
                    "legacy_rto_name": legacy_names.get(legacy_code, ""),
                    "link_status": LINK_LINKED if has_history else LINK_PORTAL_ONLY,
                }
            )

    # V1 offices the portal no longer lists — kept as rows so they are visibly
    # accounted for rather than silently dropped from the crosswalk.
    for legacy_code, name in sorted(legacy_names.items()):
        if legacy_code in seen_legacy_codes:
            continue
        state = legacy_states[legacy_code]
        output_rows.append(
            {
                "state": state,
                "state_code": STATE_NAME_TO_NEW_PORTAL_CODE.get(state, ""),
                "new_rto_code": "",
                "new_rto_name": "",
                "legacy_rto_code": legacy_code,
                "legacy_rto_name": name,
                "link_status": LINK_LEGACY_ONLY,
            }
        )

    return output_rows


def write_crosswalk(rows: list[dict], output_path: Path = OUTPUT_PATH) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "state",
        "state_code",
        "new_rto_code",
        "new_rto_name",
        "legacy_rto_code",
        "legacy_rto_name",
        "link_status",
    ]
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    counts = {status: 0 for status in (LINK_LINKED, LINK_PORTAL_ONLY, LINK_LEGACY_ONLY)}
    for row in rows:
        counts[row["link_status"]] += 1
    logger.info(
        "Wrote %s rows to %s (%s linked, %s portal-only, %s legacy-only)",
        len(rows),
        output_path,
        counts[LINK_LINKED],
        counts[LINK_PORTAL_ONLY],
        counts[LINK_LEGACY_ONLY],
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--no-db",
        action="store_true",
        help=(
            "Skip the DB query; emit the portal side only. Every row is then "
            "portal_only and legacy office names are blank."
        ),
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    legacy_rtos: list[tuple] = []
    if args.no_db:
        logger.warning("--no-db: legacy names and legacy_only rows will be missing")
    else:
        conn = connect()
        try:
            legacy_rtos = fetch_legacy_rtos(conn.cursor())
        finally:
            conn.close()

    portal = NewPortalClient()
    portal.start_session()

    write_crosswalk(build_crosswalk_rows(legacy_rtos, portal))


if __name__ == "__main__":
    main()
