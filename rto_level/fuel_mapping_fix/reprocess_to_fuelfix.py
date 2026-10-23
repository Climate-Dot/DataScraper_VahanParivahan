"""One-off: reprocess RTO history with the corrected ethanol/E20 fuel mapping.

Writes to fact_ev_data_by_rto_fuelfix, NEVER to fact_ev_data_by_rto. Promotion to
the production table is a separate, later decision.

Uses rto_preprocessing_fuelfix (a subclass), not the production preprocessing
script, and its own CSV prefix. Nothing the cron job runs is modified.

Why this exists: Vahan renamed two fuel headers effective May 2026
("ETHANOL" -> "ETHANOL(E100)", "PETROL/ETHANOL" -> "PETROL(E20)"). Only the new
spellings are in COMMON_FUEL_COLUMN_RENAME_MAP, so for reports dated April 2026
and earlier those columns were dropped and stored as NULL. The legacy headers are
recovered here into their OWN columns (ethanol, petrol_ethanol) rather than being
folded into the current ones - see rto_preprocessing_fuelfix for why.

Deliberately does NOT run upload_files_to_blob_storage.py: that step deletes the
local raw XLSX files, which are the only input this reprocess has.

Resumable: every completed month is appended to a progress file and skipped on a
later run.

Usage:
    python3 rto_level/fuel_mapping_fix/reprocess_to_fuelfix.py 2013 2026
    python3 rto_level/fuel_mapping_fix/reprocess_to_fuelfix.py 2025 2025
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
from rto_level.fuel_mapping_fix.rto_preprocessing_fuelfix import FILE_PREFIX

configure_pipeline_logging()

MONTHS = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN",
          "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
PROGRESS_PATH = os.path.join(os.path.dirname(__file__), "progress.txt")


class FuelFixIngestor(BaseSqlServerIngestor):
    """Identical to RtoDataIngest except for the target tables."""

    def __init__(self):
        super().__init__(
            file_prefix=FILE_PREFIX,
            staging_table_name="staging_fact_ev_data_by_rto_fuelfix",
            final_table_name="fact_ev_data_by_rto_fuelfix",
            merge_key_columns=["date", "state", "rto_code", "rto_name", "vehicle_class"],
            missing_file_hint="data_pre_processing",
        )


def done_months():
    if not os.path.exists(PROGRESS_PATH):
        return set()
    with open(PROGRESS_PATH) as handle:
        return {line.split()[0] for line in handle if line.strip()}


def record(key, rows, seconds):
    with open(PROGRESS_PATH, "a") as handle:
        handle.write(f"{key} rows={rows} seconds={seconds:.1f}\n")


def main():
    start_year, end_year = int(sys.argv[1]), int(sys.argv[2])
    already = done_months()
    ingestor = FuelFixIngestor()

    for year in range(start_year, end_year + 1):
        for month in MONTHS:
            key = f"{year}-{month}"
            if key in already:
                print(f"[skip] {key} already done", flush=True)
                continue

            started = time.time()
            print(f"[{key}] preprocessing...", flush=True)
            result = subprocess.run(
                [sys.executable,
                 "rto_level/fuel_mapping_fix/rto_preprocessing_fuelfix.py",
                 month, str(year)],
                cwd=repo_path,
                capture_output=True,
                text=True,
                env={**os.environ, "PYTHONPATH": repo_path},
            )
            if result.returncode != 0:
                print(f"[{key}] PREPROCESS FAILED\n{result.stderr[-1500:]}", flush=True)
                continue

            csv_path = os.path.join(repo_path, f"{FILE_PREFIX}_{month}_{year}.csv")
            if not os.path.exists(csv_path):
                print(f"[{key}] no CSV produced - no raw files for this month, skipping",
                      flush=True)
                record(key, 0, time.time() - started)
                continue

            print(f"[{key}] ingesting...", flush=True)
            try:
                rows = ingestor.data_ingest(month, str(year))
            except Exception as exc:  # noqa: BLE001 - log and continue to next month
                print(f"[{key}] INGEST FAILED: {exc}", flush=True)
                continue

            elapsed = time.time() - started
            record(key, rows, elapsed)
            print(f"[{key}] done rows={rows} in {elapsed:.0f}s", flush=True)
            os.remove(csv_path)


if __name__ == "__main__":
    main()
