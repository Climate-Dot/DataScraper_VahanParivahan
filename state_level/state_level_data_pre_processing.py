import os
import sys

import pandas as pd

repo_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if repo_path not in sys.path:
    sys.path.append(repo_path)

from etl_preprocessing import BaseExcelPreprocessor, ReportContext
from pipeline_constants import COMMON_FUEL_COLUMN_RENAME_MAP
from pipeline_logging import configure_pipeline_logging
from runtime_config import get_previous_month_year_label, resolve_month_year_args

configure_pipeline_logging()


class StateLevelDataPreProcessor(BaseExcelPreprocessor):
    def __init__(self):
        self.column_rename_map = {
            "Year": "year",
            "Month": "month",
            "Day": "day",
            "Date": "date",
            "State": "state",
            "Vehicle Type": "vehicle_type",
            "Vehicle Category": "vehicle_category",
            "Vehicle Use Type": "vehicle_use_type",
            "Unnamed: 1": "vehicle_class",
            **COMMON_FUEL_COLUMN_RENAME_MAP,
        }
        super().__init__(
            pipeline_label="state",
            base_directory=os.getcwd(),
            raw_relative_path=os.path.join("state_level", "state_level_ev_data"),
            column_rename_map=self.column_rename_map,
        )

    @staticmethod
    def get_year_month_label():
        return get_previous_month_year_label()

    def iter_report_contexts(self, month, year, **kwargs):
        if not self.raw_files_directory.exists():
            return

        for state_dir in sorted(
            path for path in self.raw_files_directory.iterdir() if path.is_dir()
        ):
            yield ReportContext(
                report_path=state_dir / year / month / "reportTable.xlsx",
                metadata={
                    "Month": month,
                    "Year": year,
                    "Day": 1,
                    "Date": f"1/{month}/{year}",
                    "State": state_dir.name,
                },
                labels={
                    "state": state_dir.name,
                    "year": year,
                    "month": month,
                },
            )

    def apply_mapping(self, df):
        return pd.merge(
            df,
            self.mapping_df,
            left_on="Unnamed: 1",
            right_on="Vehicle Class",
            how="left",
        )

    def data_preprocessing(self, month, year):
        return self.run_preprocessing(month, year)


def main():
    state_level_data_preprocessor = StateLevelDataPreProcessor()
    month, year = resolve_month_year_args(sys.argv[1:])
    final_df = state_level_data_preprocessor.data_preprocessing(month, year)
    final_df.to_csv(
        f"state_level_ev_data_{month}_{year}.csv", header=True, index=False
    )


if __name__ == "__main__":
    main()
