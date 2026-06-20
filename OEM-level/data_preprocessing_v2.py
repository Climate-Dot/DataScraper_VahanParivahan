from datetime import datetime, timedelta
import logging
import os
import pandas as pd
import re
import sys
import warnings

warnings.filterwarnings("ignore")

repo_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if repo_path not in sys.path:
    sys.path.append(repo_path)

from preprocessing_schema_utils import (
    ensure_expected_output_columns,
    find_unexpected_source_columns,
)
from pipeline_constants import COMMON_FUEL_COLUMN_RENAME_MAP


class OEMDataPreProcessor:
    def __init__(self):
        self.mapping_file_path = os.path.join(os.getcwd(), "Table and Mapping V2.xlsx")
        self.mapping_df = pd.read_excel(self.mapping_file_path, sheet_name="Mapping")
        self.raw_files_directory = os.path.join(
            os.getcwd(), "OEM-level", "oem_data_by_state_and_category"
        )
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

        self.month_mapping = {
            "JAN": 1,
            "FEB": 2,
            "MAR": 3,
            "APR": 4,
            "MAY": 5,
            "JUN": 6,
            "JUL": 7,
            "AUG": 8,
            "SEP": 9,
            "OCT": 10,
            "NOV": 11,
            "DEC": 12,
        }

    @staticmethod
    def get_year_month_label():
        """
        Get month and year label for OEM scraper.
        :return:
        """
        # Get the current date
        current_date = datetime.now()
        # Calculate the first day of the current month
        first_day_of_current_month = current_date.replace(day=1)
        # Calculate the last day of the previous month
        last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
        # Get the month abbreviation and year for the last day of the previous month
        month_abbreviation = last_day_of_previous_month.strftime("%b")
        year = last_day_of_previous_month.strftime("%Y")
        return month_abbreviation.upper(), year

    @staticmethod
    def remove_special_chars(text):
        return re.sub(r"\W+", " ", text)

    @staticmethod
    def convert_date(date_str):
        return datetime.strptime(date_str, "%d/%b/%Y").strftime("%d/%m/%Y")

    def data_preprocessing(self, month, year):
        """
        function to process oem data files
        :param month: month of the file to pre_process
        :param year: year of the file to pre_process
        :return: None
        """
        final_df = pd.DataFrame()
        unexpected_source_columns = set()
        for state in os.listdir(self.raw_files_directory):
            state_path = os.path.join(self.raw_files_directory, state)
            for vehicle_class in os.listdir(state_path):
                raw_file_path = os.path.join(
                    state_path, vehicle_class, year, month, "reportTable.xlsx"
                )
                if os.path.exists(raw_file_path):
                    temp_df = pd.read_excel(raw_file_path, skiprows=3, index_col=0)
                    if temp_df.empty:
                        print(
                            f"No records found in report for {vehicle_class}, {state}, {year}, {month}"
                        )
                    else:
                        unexpected_source_columns.update(
                            find_unexpected_source_columns(
                                temp_df.columns,
                                self.column_rename_map.keys(),
                            )
                        )
                        temp_df["Month"] = month
                        temp_df["Year"] = year
                        temp_df["Day"] = 1
                        temp_df["Date"] = f"{1}/{month}/{year}"
                        temp_df["Vehicle Class"] = vehicle_class
                        temp_df["State"] = state
                        final_df = pd.concat([final_df, temp_df], ignore_index=True)
        if unexpected_source_columns:
            logging.warning(
                "Detected new OEM source columns for %s %s that are not mapped yet: %s",
                month,
                year,
                ", ".join(sorted(unexpected_source_columns)),
            )
        # create vehicle category and vehicle type columns
        self.mapping_df["Vehicle Class"] = self.mapping_df["Vehicle Class"].apply(
            self.remove_special_chars
        )
        self.mapping_df["Vehicle Class"] = self.mapping_df["Vehicle Class"].str.strip()
        final_df = pd.merge(final_df, self.mapping_df, on="Vehicle Class", how="left")
        # Replace NaN values with 'Others' for 'Vehicle Category' and 'Vehicle Type' columns
        final_df["Vehicle Type"] = (
            final_df["Vehicle Type"].fillna("Others").replace("", "Others")
        )
        final_df["Vehicle Category"] = (
            final_df["Vehicle Category"].fillna("Others").replace("", "Others")
        )
        final_df["Vehicle Use Type"] = (
            final_df["Vehicle Use Type"].fillna("Others").replace("", "Others")
        )
        # Replace values in state column
        value_replacements = {
            "Andaman   Nicobar Island": "Andaman and Nicobar",
            "UT of DNH and DD": "Dadara and Nagar Havelli",
            # Add more replacements as needed
        }
        final_df["State"] = final_df["State"].replace(value_replacements)
        # rename columns
        final_df = final_df.rename(self.column_rename_map, axis=1)

        final_df["date"] = final_df["date"].apply(self.convert_date)
        final_df["month"] = final_df["month"].map(self.month_mapping)
        final_df = ensure_expected_output_columns(
            final_df,
            self.column_rename_map.values(),
            f"OEM preprocessing for {month} {year}",
        )

        # reorder columns
        final_df = final_df[self.column_rename_map.values()]
        return final_df


def main():
    oem_level_data_preprocessor = OEMDataPreProcessor()
    if len(sys.argv) > 2:
        # If month and year are passed as command-line arguments
        month = sys.argv[1]
        year = sys.argv[2]
    else:
        # Use the default function to get the month and year
        month, year = oem_level_data_preprocessor.get_year_month_label()
    final_df = oem_level_data_preprocessor.data_preprocessing(month, year)
    final_df.to_csv(
        f"oem_data_by_state_and_category_{month}_{year}.csv", header=True, index=False
    )


if __name__ == "__main__":
    main()
