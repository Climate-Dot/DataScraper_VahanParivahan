"""Regenerates climate_dot_dbt/seeds/vehicle_class_crosswalk.csv from the
authoritative business mapping in "Table and Mapping V2.xlsx" (Mapping
sheet) — the same file V1's own preprocessing pipeline already joins
against at ingestion time (see rto_level_data_pre_processing.py's
apply_mapping(): `pd.merge(df, mapping_df, left_on="Unnamed: 1",
right_on="Vehicle Class", how="left")`).

Correction, 2026-08-14 (Monish): vehicle_type/vehicle_category/
vehicle_use_type were never raw Vahan columns, on the old portal or the new
one — only vehicle_class ever was. The other three are a business-defined
categorization laid on top via that left join, with unmatched classes
defaulting to "Others" (etl_preprocessing.DEFAULT_DIMENSION_VALUE). An
earlier version of this script instead reverse-engineered the mapping from
historical DB values — functionally near-identical (0 mismatches on 72
classes shared with the XLSX) but the wrong source of truth, and it masked
the fact that unmapped classes should explicitly default to "Others" rather
than whatever the DB's own historical left-join fallback happened to
produce.

Run on the VM, where pyodbc and the real DB credentials are available (only
needed to check *coverage* — which classes have actually been observed — not
to derive the mapping values themselves):
    python3 -m new_portal.build_vehicle_class_crosswalk
"""

from __future__ import annotations

import csv
import logging
import re
from pathlib import Path

import pandas as pd

try:
    import pyodbc
except ImportError:  # pragma: no cover - pyodbc needs system ODBC libs absent in CI
    pyodbc = None

from runtime_config import load_config
from sqlserver_utils import connect_with_retry

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent
MAPPING_FILE_PATH = REPO_ROOT / "Table and Mapping V2.xlsx"
OUTPUT_PATH = REPO_ROOT / "climate_dot_dbt" / "seeds" / "vehicle_class_crosswalk.csv"

SOURCE_TABLE = "fact_ev_data_by_rto"
DEFAULT_DIMENSION_VALUE = "Others"  # matches etl_preprocessing.DEFAULT_DIMENSION_VALUE

SOURCE_AUTHORITATIVE = "authoritative_mapping_file"
SOURCE_DEFAULTED = "defaulted_others"


def normalize_class_name(name: str) -> str:
    """Uppercase, strip punctuation, collapse whitespace.

    Handles cosmetic differences between the mapping file and observed data
    (e.g. "ROAD ROLLER" vs "ROAD ROLLERS", "TOW TRUCK" vs "TOE TRUCK") —
    both pairs resolve to the same (Others, Others, Others) target anyway,
    but matching robustly avoids treating them as two different classes.
    """
    without_punctuation = re.sub(r"[^A-Za-z0-9\s]", " ", name.upper())
    return re.sub(r"\s+", " ", without_punctuation).strip()


def load_authoritative_mapping(path: Path = MAPPING_FILE_PATH) -> dict[str, tuple]:
    """{normalized_class: (vehicle_class, vehicle_type, vehicle_category, vehicle_use_type)}."""
    mapping_df = pd.read_excel(path, sheet_name="Mapping")
    mapping: dict[str, tuple] = {}
    duplicates = []
    for _, row in mapping_df.iterrows():
        vehicle_class = str(row["Vehicle Class"]).strip()
        key = normalize_class_name(vehicle_class)
        combo = (
            vehicle_class,
            row["Vehicle Type"],
            row["Vehicle Category"],
            row["Vehicle Use Type"],
        )
        if key in mapping and mapping[key] != combo:
            duplicates.append((vehicle_class, mapping[key], combo))
        mapping[key] = combo

    if duplicates:
        raise ValueError(
            f"{path} has conflicting rows for the same vehicle class: {duplicates}"
        )
    return mapping


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


def fetch_observed_vehicle_classes(cursor) -> list[str]:
    """Classes actually seen in production data — used only to check the
    mapping file's coverage, not to derive its values."""
    cursor.execute(f"SELECT DISTINCT vehicle_class FROM {SOURCE_TABLE} ORDER BY vehicle_class")
    return [row[0] for row in cursor.fetchall()]


def build_crosswalk_rows(
    authoritative_mapping: dict[str, tuple], observed_classes: list[str]
) -> list[dict]:
    rows = []
    for vehicle_class in observed_classes:
        key = normalize_class_name(vehicle_class)
        matched = authoritative_mapping.get(key)
        if matched is not None:
            _, vehicle_type, vehicle_category, vehicle_use_type = matched
            source = SOURCE_AUTHORITATIVE
        else:
            logger.warning(
                "%r has observed production data but is not in %s — defaulting to %r "
                "(matches etl_preprocessing.py's own left-join fallback behavior)",
                vehicle_class,
                MAPPING_FILE_PATH.name,
                DEFAULT_DIMENSION_VALUE,
            )
            vehicle_type = vehicle_category = vehicle_use_type = DEFAULT_DIMENSION_VALUE
            source = SOURCE_DEFAULTED

        rows.append(
            {
                "vehicle_class": vehicle_class,
                "vehicle_type": vehicle_type,
                "vehicle_category": vehicle_category,
                "vehicle_use_type": vehicle_use_type,
                "source": source,
            }
        )
    return rows


def write_crosswalk(rows: list[dict], output_path: Path = OUTPUT_PATH) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["vehicle_class", "vehicle_type", "vehicle_category", "vehicle_use_type", "source"]
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    defaulted = sum(1 for r in rows if r["source"] == SOURCE_DEFAULTED)
    logger.info(
        "Wrote %s rows to %s (%s from the mapping file, %s defaulted to %r)",
        len(rows),
        output_path,
        len(rows) - defaulted,
        defaulted,
        DEFAULT_DIMENSION_VALUE,
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    authoritative_mapping = load_authoritative_mapping()

    conn = connect()
    try:
        cursor = conn.cursor()
        observed_classes = fetch_observed_vehicle_classes(cursor)
    finally:
        conn.close()

    rows = build_crosswalk_rows(authoritative_mapping, observed_classes)
    write_crosswalk(rows)


if __name__ == "__main__":
    main()
