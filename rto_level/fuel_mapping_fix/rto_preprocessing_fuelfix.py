"""One-off RTO preprocessing that keeps the legacy fuel headers as their own columns.

NOT used by cron. The production path (rto_level/rto_level_data_pre_processing.py)
is untouched; this subclasses it so the two share all behaviour except the fuel
column map.

Background: Vahan renamed two fuel-column headers effective May 2026. Reports
dated April 2026 and earlier carry:

    "ETHANOL"         and  "PETROL/ETHANOL"

while May 2026 onward carry:

    "ETHANOL(E100)"   and  "PETROL(E20)"

Only the current spellings are in COMMON_FUEL_COLUMN_RENAME_MAP, so the legacy
columns were dropped at preprocessing and stored as NULL. Measured loss against
the reported Total: ~0% through 2022 (the columns existed but were empty), 1.3%
in 2023, 2.0% in 2024, ~39% by Dec 2025, ~40% in early 2026 — and it varies
0%-27% BY STATE, because states re-scraped after the rename already carry the
new spellings.

This maps the legacy headers to their OWN output columns rather than folding
them into petrol_e20 / ethanol_e100:

    "ETHANOL"        -> ethanol
    "PETROL/ETHANOL" -> petrol_ethanol

That is deliberate. Treating "PETROL/ETHANOL" as equivalent to "PETROL(E20)"
would be an inference we cannot verify from the source, and this repo's rule is
to preserve new Vahan columns as first-class columns rather than force them into
legacy aliases. Keeping them separate also avoids a real failure mode: the base
class renames columns AFTER pd.concat combines a month's offices, so mapping two
source headers onto one output name produces two identically-named columns in
any month that mixes old- and new-format reports, which breaks numeric coercion.

Whether the two pairs should later be merged is a business decision, made with
both columns visible in the data.

Usage:
    python3 rto_level/fuel_mapping_fix/rto_preprocessing_fuelfix.py JUN 2025
"""

from __future__ import annotations

import os
import sys

import pandas as pd

repo_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if repo_path not in sys.path:
    sys.path.append(repo_path)

from etl_preprocessing import BaseExcelPreprocessor
from pipeline_logging import configure_pipeline_logging
from runtime_config import resolve_month_year_args

sys.path.append(os.path.join(repo_path, "rto_level"))
from rto_level.rto_level_data_pre_processing import RTOLevelDataPreProcessor

configure_pipeline_logging()

# Legacy source header -> its own output column. Not folded into the current
# columns; see the module docstring.
LEGACY_FUEL_COLUMNS = {
    "ETHANOL": "ethanol",
    "PETROL/ETHANOL": "petrol_ethanol",
}

FILE_PREFIX = "rto_level_ev_data_fuelfix"


class FuelFixRTOPreProcessor(RTOLevelDataPreProcessor):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # BaseExcelPreprocessor keeps its own dict, and output_columns is a
        # property over it, so extending here adds the two output columns too.
        self.column_rename_map.update(LEGACY_FUEL_COLUMNS)

    @staticmethod
    def _coerce_numeric_output_columns(df: pd.DataFrame) -> pd.DataFrame:
        """Coerce the standard fuel columns, then the two legacy ones.

        The base implementation iterates a module-level frozenset built from
        COMMON_FUEL_COLUMN_RENAME_MAP, which cannot see these two columns.
        """
        df = BaseExcelPreprocessor._coerce_numeric_output_columns(df)

        for column_name in LEGACY_FUEL_COLUMNS.values():
            if column_name not in df.columns:
                continue
            # Same normalization the base class uses: Excel cells arrive as a
            # mix of Python numbers and comma-formatted strings, and the pandas
            # .str accessor silently turns numeric objects into NaN, so cast to
            # nullable strings before stripping separators.
            series = df[column_name].astype("string")
            series = series.str.replace(",", "", regex=False)
            df[column_name] = pd.to_numeric(series, errors="coerce").astype("Int64")

        return df


def main():
    preprocessor = FuelFixRTOPreProcessor()
    month, year = resolve_month_year_args(sys.argv[1:])
    final_df = preprocessor.data_preprocessing(month, year)
    final_df.to_csv(f"{FILE_PREFIX}_{month}_{year}.csv", header=True, index=False)


if __name__ == "__main__":
    main()
