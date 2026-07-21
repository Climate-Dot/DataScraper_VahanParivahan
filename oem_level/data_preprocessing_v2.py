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
from utils import remove_special_chars

configure_pipeline_logging()


class OEMDataPreProcessor(BaseExcelPreprocessor):
    def __init__(self):
        self.column_rename_map = {
            "Year": "year",
            "Month": "month",
            "Day": "day",
            "Date": "date",
            "State": "state",
            "Vehicle Class": "vehicle_class",
            "Vehicle Type": "vehicle_type",
            "Vehicle Category": "vehicle_category",
            "Vehicle Use Type": "vehicle_use_type",
            "Unnamed: 1": "maker",
            **COMMON_FUEL_COLUMN_RENAME_MAP,
        }
        super().__init__(
            pipeline_label="OEM",
            base_directory=os.getcwd(),
            raw_relative_path=os.path.join(
                "oem_level",
                "oem_data_by_state_and_category",
            ),
            column_rename_map=self.column_rename_map,
        )

    def load_mapping_df(self):
        mapping_df = super().load_mapping_df()
        mapping_df["Vehicle Class"] = mapping_df["Vehicle Class"].apply(
            remove_special_chars
        )
        mapping_df["Vehicle Class"] = mapping_df["Vehicle Class"].str.strip()
        return mapping_df

    @staticmethod
    def get_year_month_label():
        return get_previous_month_year_label()

    def iter_report_contexts(self, month, year, **kwargs):
        if not self.raw_files_directory.exists():
            return

        for state_dir in sorted(
            path for path in self.raw_files_directory.iterdir() if path.is_dir()
        ):
            for vehicle_class_dir in sorted(
                path for path in state_dir.iterdir() if path.is_dir()
            ):
                yield ReportContext(
                    report_path=vehicle_class_dir / year / month / "reportTable.xlsx",
                    metadata={
                        "Month": month,
                        "Year": year,
                        "Day": 1,
                        "Date": f"1/{month}/{year}",
                        "Vehicle Class": vehicle_class_dir.name,
                        "State": state_dir.name,
                    },
                    labels={
                        "state": state_dir.name,
                        "vehicle_class": vehicle_class_dir.name,
                        "year": year,
                        "month": month,
                    },
                )

    def apply_mapping(self, df):
        return pd.merge(df, self.mapping_df, on="Vehicle Class", how="left")

    def data_preprocessing(self, month, year):
        return self.run_preprocessing(month, year)


def main():
    oem_level_data_preprocessor = OEMDataPreProcessor()
    month, year = resolve_month_year_args(sys.argv[1:])
    final_df = oem_level_data_preprocessor.data_preprocessing(month, year)
    final_df.to_csv(
        f"oem_data_by_state_and_category_{month}_{year}.csv", header=True, index=False
    )


if __name__ == "__main__":
    main()
