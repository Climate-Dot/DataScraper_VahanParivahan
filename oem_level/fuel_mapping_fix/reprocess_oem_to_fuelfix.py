"""One-off: reprocess OEM history with the corrected ethanol/E20 fuel mapping.

Writes to fact_oem_data_by_state_and_category_fuelfix, NEVER to the production
table. Promotion is a separate, later step and is an UPDATE of two columns, not
a table swap, so inserted_at and every other column are preserved.

Unlike the RTO equivalent this needs no preprocessing subclass: once the legacy
headers were added to COMMON_FUEL_COLUMN_RENAME_MAP, the production OEM
preprocessor emits `ethanol` and `petrol_ethanol` itself.

Scope is 2013-01 to 2026-04. From 2026-05 the source uses the new header
spellings, so later months have nothing to recover. Pre-2023 months recover only
a small amount (~23,000 vehicles in total) but are included because OEM's legacy
PETROL/ETHANOL column does carry data that far back.

Deliberately does NOT run upload_files_to_blob_storage.py: that step deletes the
local raw XLSX files, which are this reprocess's only input.

Resumable: each completed month is appended to a progress file and skipped later.

Usage:
    python3 oem_level/fuel_mapping_fix/reprocess_oem_to_fuelfix.py 2023 2026
"""

import os
import subprocess
import sys
import time

repo_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if repo_path not in sys.path:
    sys.path.append(repo_path)

from etl_ingestion import BaseSqlServerIngestor
from pipeline_logging import configure_pipeline_logging

configure_pipeline_logging()

MONTHS = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN",
          "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
# Months outside this window have nothing to recover; see the module docstring.
# Extended back to 2013 on 2026-10-23: OEM's legacy PETROL/ETHANOL column turned
# out to carry data as far back as 2014 (e.g. one Tamil Nadu row in 2014-01 holds
# 17,687 vehicles), unlike RTO where pre-2023 was effectively empty. The
# recoverable pre-2023 total is small (~23,000 vehicles) but it is what keeps the
# fuel columns from reconciling to total in those years.
FIRST = (2013, "JAN")
LAST = (2026, "APR")
FILE_PREFIX = "oem_data_by_state_and_category"
PROGRESS_PATH = os.path.join(os.path.dirname(__file__), "progress.txt")


class OemFuelFixIngestor(BaseSqlServerIngestor):
    """Identical to OEMDataIngest except for the target tables."""

    def __init__(self):
        super().__init__(
            file_prefix=FILE_PREFIX,
            staging_table_name="staging_fact_oem_data_by_state_and_category_fuelfix",
            final_table_name="fact_oem_data_by_state_and_category_fuelfix",
            merge_key_columns=["date", "state", "vehicle_class"],
            missing_file_hint="data_preprocessing_v2",
        )


def ordered_months(start_year, end_year):
    for year in range(start_year, end_year + 1):
        for month in MONTHS:
            key = (year, MONTHS.index(month))
            if key < (FIRST[0], MONTHS.index(FIRST[1])):
                continue
            if key > (LAST[0], MONTHS.index(LAST[1])):
                return
            yield year, month


def done_months():
    if not os.path.exists(PROGRESS_PATH):
        return set()
    with open(PROGRESS_PATH) as handle:
        return {line.split()[0] for line in handle if line.strip()}


def main():
    start_year, end_year = int(sys.argv[1]), int(sys.argv[2])
    already = done_months()
    ingestor = OemFuelFixIngestor()

    for year, month in ordered_months(start_year, end_year):
        key = f"{year}-{month}"
        if key in already:
            print(f"[skip] {key}", flush=True)
            continue

        started = time.time()
        print(f"[{key}] preprocessing...", flush=True)
        result = subprocess.run(
            [sys.executable, "oem_level/data_preprocessing_v2.py", month, str(year)],
            cwd=repo_path, capture_output=True, text=True,
            env={**os.environ, "PYTHONPATH": repo_path},
        )
        if result.returncode != 0:
            print(f"[{key}] PREPROCESS FAILED\n{result.stderr[-1500:]}", flush=True)
            continue

        csv_path = os.path.join(repo_path, f"{FILE_PREFIX}_{month}_{year}.csv")
        if not os.path.exists(csv_path):
            print(f"[{key}] no CSV produced - no raw files, skipping", flush=True)
            with open(PROGRESS_PATH, "a") as handle:
                handle.write(f"{key} rows=0 seconds=0\n")
            continue

        print(f"[{key}] ingesting...", flush=True)
        try:
            rows = ingestor.data_ingest(month, str(year))
        except Exception as exc:  # noqa: BLE001 - log and continue to next month
            print(f"[{key}] INGEST FAILED: {exc}", flush=True)
            continue

        elapsed = time.time() - started
        with open(PROGRESS_PATH, "a") as handle:
            handle.write(f"{key} rows={rows} seconds={elapsed:.1f}\n")
        print(f"[{key}] done rows={rows} in {elapsed:.0f}s", flush=True)
        os.remove(csv_path)


if __name__ == "__main__":
    main()
