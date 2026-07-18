from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator

import pandas as pd

from pipeline_constants import MONTH_NAME_TO_NUMBER
from preprocessing_schema_utils import (
    ensure_expected_output_columns,
    find_unexpected_source_columns,
)
from utils import convert_date, is_valid_excel_download

DEFAULT_DIMENSION_VALUE = "Others"
STATE_VALUE_REPLACEMENTS = {
    "Andaman   Nicobar Island": "Andaman and Nicobar",
    "UT of DNH and DD": "Dadara and Nagar Havelli",
}


@dataclass(frozen=True)
class ReportContext:
    report_path: Path
    metadata: dict[str, object]
    labels: dict[str, str]


class BaseExcelPreprocessor:
    def __init__(
        self,
        *,
        pipeline_label: str,
        base_directory: str | Path,
        raw_relative_path: str | Path,
        column_rename_map: dict[str, str],
        mapping_file_path: str | Path | None = None,
        raw_files_directory: str | Path | None = None,
    ) -> None:
        self.pipeline_label = pipeline_label
        self.base_directory = Path(base_directory)
        self.mapping_file_path = (
            Path(mapping_file_path)
            if mapping_file_path is not None
            else self.base_directory / "Table and Mapping V2.xlsx"
        )
        self.mapping_df = self.load_mapping_df()
        self.raw_files_directory = (
            Path(raw_files_directory)
            if raw_files_directory is not None
            else self.base_directory / raw_relative_path
        )
        self.column_rename_map = dict(column_rename_map)

    def load_mapping_df(self) -> pd.DataFrame:
        return pd.read_excel(self.mapping_file_path, sheet_name="Mapping")

    @property
    def output_columns(self) -> list[str]:
        return list(self.column_rename_map.values())

    def iter_report_contexts(
        self,
        month: str,
        year: str,
        **kwargs,
    ) -> Iterator[ReportContext]:
        raise NotImplementedError

    def apply_mapping(self, df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError

    def describe_scope(self, month: str, year: str, **kwargs) -> str:
        return "requested scope"

    def build_empty_report_log_message(self, context: ReportContext) -> str:
        details = " ".join(
            f"{key}={value}" for key, value in context.labels.items() if value != ""
        )
        return f"No records found in {self.pipeline_label} report for {details}"

    def log_unexpected_source_columns(
        self,
        month: str,
        year: str,
        unexpected_source_columns: Iterable[str],
    ) -> None:
        logging.warning(
            "Detected new %s source columns for %s %s that are not mapped yet: %s",
            self.pipeline_label,
            month,
            year,
            ", ".join(sorted(unexpected_source_columns)),
        )

    def normalize_enriched_frame(self, df: pd.DataFrame) -> pd.DataFrame:
        for column_name in ("Vehicle Type", "Vehicle Category", "Vehicle Use Type"):
            if column_name in df.columns:
                df[column_name] = (
                    df[column_name]
                    .fillna(DEFAULT_DIMENSION_VALUE)
                    .replace("", DEFAULT_DIMENSION_VALUE)
                )

        if "State" in df.columns:
            df["State"] = df["State"].replace(STATE_VALUE_REPLACEMENTS)

        return df

    def finalize_output(self, df: pd.DataFrame, month: str, year: str) -> pd.DataFrame:
        df = self.apply_mapping(df)
        df = self.normalize_enriched_frame(df)
        df = df.rename(self.column_rename_map, axis=1)

        if "date" in df.columns:
            df["date"] = df["date"].apply(convert_date)

        if "month" in df.columns:
            df["month"] = df["month"].map(MONTH_NAME_TO_NUMBER)

        df = ensure_expected_output_columns(
            df,
            self.output_columns,
            f"{self.pipeline_label.upper()} preprocessing for {month} {year}",
        )
        return df[self.output_columns]

    def _read_report(self, report_path: Path) -> pd.DataFrame:
        if not is_valid_excel_download(report_path):
            raise ValueError(
                f"Invalid {self.pipeline_label} Excel report file: {report_path}"
            )

        return pd.read_excel(
            report_path,
            skiprows=3,
            index_col=0,
            engine="openpyxl",
        )

    def run_preprocessing(self, month: str, year: str, **kwargs) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []
        unexpected_source_columns: set[str] = set()
        files_found = 0
        empty_reports = 0

        for context in self.iter_report_contexts(month, year, **kwargs):
            if not context.report_path.exists():
                continue

            files_found += 1
            temp_df = self._read_report(context.report_path)
            if temp_df.empty:
                empty_reports += 1
                logging.warning(self.build_empty_report_log_message(context))
                continue

            unexpected_source_columns.update(
                find_unexpected_source_columns(
                    temp_df.columns,
                    self.column_rename_map.keys(),
                )
            )

            enriched_df = temp_df.copy()
            for key, value in context.metadata.items():
                enriched_df[key] = value
            frames.append(enriched_df)

        if unexpected_source_columns:
            self.log_unexpected_source_columns(month, year, unexpected_source_columns)

        if not frames:
            logging.warning(
                "No non-empty %s rows were produced for %s %s in %s.",
                self.pipeline_label,
                month,
                year,
                self.describe_scope(month, year, **kwargs),
            )
            return pd.DataFrame(columns=self.output_columns)

        final_df = pd.concat(frames, ignore_index=True)
        final_df = self.finalize_output(final_df, month, year)

        logging.info(
            "%s preprocessing summary for %s %s: files_found=%s empty_reports=%s output_rows=%s",
            self.pipeline_label.upper(),
            month,
            year,
            files_found,
            empty_reports,
            len(final_df),
        )
        return final_df
