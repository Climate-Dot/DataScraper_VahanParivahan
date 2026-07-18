from __future__ import annotations

import os
import sys
from typing import Iterable

import pandas as pd

repo_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if repo_path not in sys.path:
    sys.path.append(repo_path)

from etl_preprocessing import BaseExcelPreprocessor, ReportContext
from pipeline_constants import COMMON_FUEL_COLUMN_RENAME_MAP
from pipeline_logging import configure_pipeline_logging
from runtime_config import resolve_month_year_args

configure_pipeline_logging()


class RTOLevelDataPreProcessor(BaseExcelPreprocessor):
    def __init__(
        self,
        *,
        base_directory: str | os.PathLike[str] | None = None,
        raw_files_directory: str | os.PathLike[str] | None = None,
        mapping_file_path: str | os.PathLike[str] | None = None,
    ):
        self.column_rename_map = {
            "Year": "year",
            "Month": "month",
            "Day": "day",
            "Date": "date",
            "State": "state",
            "rto_name": "rto_name",
            "rto_code": "rto_code",
            "Vehicle Type": "vehicle_type",
            "Vehicle Category": "vehicle_category",
            "Vehicle Use Type": "vehicle_use_type",
            "Unnamed: 1": "vehicle_class",
            **COMMON_FUEL_COLUMN_RENAME_MAP,
        }
        super().__init__(
            pipeline_label="RTO",
            base_directory=base_directory or os.getcwd(),
            raw_relative_path=os.path.join("rto_level", "rto_level_ev_data"),
            column_rename_map=self.column_rename_map,
            mapping_file_path=mapping_file_path,
            raw_files_directory=raw_files_directory,
        )

    def iter_report_contexts(self, month, year, **kwargs):
        selected_states = set(kwargs.get("states") or [])
        if not self.raw_files_directory.exists():
            return

        for state_dir in sorted(
            path for path in self.raw_files_directory.iterdir() if path.is_dir()
        ):
            if selected_states and state_dir.name not in selected_states:
                continue

            for rto_dir in sorted(path for path in state_dir.iterdir() if path.is_dir()):
                rto_name, rto_code = self.split_rto_folder_name(rto_dir.name)
                yield ReportContext(
                    report_path=rto_dir / year / month / "reportTable.xlsx",
                    metadata={
                        "Month": month,
                        "Year": year,
                        "Day": 1,
                        "Date": f"1/{month}/{year}",
                        "rto_name": rto_name,
                        "rto_code": rto_code,
                        "State": state_dir.name,
                    },
                    labels={
                        "state": state_dir.name,
                        "office": rto_dir.name,
                        "year": year,
                        "month": month,
                    },
                )

    @staticmethod
    def split_rto_folder_name(folder_name: str):
        if "_" not in folder_name:
            return folder_name, ""
        return folder_name.rsplit("_", 1)

    def apply_mapping(self, df):
        return pd.merge(
            df,
            self.mapping_df,
            left_on="Unnamed: 1",
            right_on="Vehicle Class",
            how="left",
        )

    def describe_scope(self, month: str, year: str, **kwargs) -> str:
        selected_states = set(kwargs.get("states") or [])
        if not selected_states:
            return "all states"
        return ", ".join(sorted(selected_states))

    def data_preprocessing(self, month, year, states: Iterable[str] | None = None):
        return self.run_preprocessing(month, year, states=states)


def main():
    rto_level_data_preprocessor = RTOLevelDataPreProcessor()
    month, year = resolve_month_year_args(sys.argv[1:])
    final_df = rto_level_data_preprocessor.data_preprocessing(month, year)
    final_df.to_csv(f"rto_level_ev_data_{month}_{year}.csv", header=True, index=False)


if __name__ == "__main__":
    main()
