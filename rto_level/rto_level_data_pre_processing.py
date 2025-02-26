import logging
import os
import pandas as pd
import sys

# configure logging to write to both the console and a file
logging.basicConfig(
    level=logging.INFO,  # Set the logging level (INFO, DEBUG, etc.)
    format="%(asctime)s - %(levelname)s - %(message)s",  # log message format
)

repo_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if repo_path not in sys.path:
    sys.path.append(repo_path)

from utils import *


class RTOLevelDataPreProcessor:
    def __init__(self):
        self.mapping_file_path = os.path.join(os.getcwd(), "Table and Mapping V2.xlsx")
        self.mapping_df = pd.read_excel(self.mapping_file_path, sheet_name="Mapping")
        self.raw_files_directory = os.path.join(
            os.getcwd(), "rto_level", "rto_level_ev_data"
        )
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
            "CNG ONLY": "cng_only",
            "DIESEL": "diesel",
            "DIESEL/HYBRID": "diesel_hybrid",
            "DI-METHYL ETHER": "di_methyl_ether",
            "DUAL DIESEL/BIO CNG": "dual_diesel_bio_cng",
            "DUAL DIESEL/CNG": "dual_diesel_cng",
            "DUAL DIESEL/LNG": "dual_diesel_lng",
            "ELECTRIC(BOV)": "electric_vehicles",
            "ETHANOL": "ethanol",
            "LNG": "lng",
            "LPG ONLY": "lpg_only",
            "METHANOL": "methanol",
            "NOT APPLICABLE": "not_applicable",
            "PETROL": "petrol",
            "PETROL/CNG": "petrol_cng",
            "PETROL/ETHANOL": "petrol_ethanol",
            "PETROL/HYBRID": "petrol_hybrid",
            "PETROL/LPG": "petrol_lpg",
            "PETROL/METHANOL": "petrol_methanol",
            "PLUG-IN HYBRID EV": "plug_in_hybrid_ev",
            "PURE EV": "pure_ev",
            "SOLAR": "solar",
            "STRONG HYBRID EV": "strong_hybrid_ev",
            "Unnamed: 26": "total"
        }

    def data_preprocessing(self, month, year):
        """
        function to process rto level data files
        :param month: month of the file to pre_process
        :param year: year of the file to pre_process
        :return: None
        """
        final_df = pd.DataFrame()
        for state in os.listdir(self.raw_files_directory):
            state_path = os.path.join(self.raw_files_directory, state)
            for rto_office in os.listdir(state_path):
                rto_office_path = os.path.join(
                    self.raw_files_directory, state, rto_office
                )
                raw_file_path = os.path.join(
                    rto_office_path, year, month, "reportTable.xlsx"
                )
                if os.path.exists(raw_file_path):
                    temp_df = pd.read_excel(raw_file_path, skiprows=3, index_col=0)
                    if temp_df.empty:
                        print(
                            f"No records found in report for {state}, {year}, {month}"
                        )
                    else:
                        temp_df["Month"] = month
                        temp_df["Year"] = year
                        temp_df["Day"] = 1
                        temp_df["Date"] = f"{1}/{month}/{year}"
                        temp_df["rto_name"] = rto_office.split("_")[0]
                        temp_df["rto_code"] = rto_office.split("_")[1]
                        temp_df["State"] = state
                        final_df = pd.concat([final_df, temp_df], ignore_index=True)
        final_df = pd.merge(
            final_df,
            self.mapping_df,
            left_on="Unnamed: 1",
            right_on="Vehicle Class",
            how="left",
        )
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

        final_df["date"] = final_df["date"].apply(convert_date)
        final_df["month"] = final_df["month"].map(month_mapping)

        # reorder columns
        final_df = final_df[self.column_rename_map.values()]
        return final_df


def main():
    rto_level_data_preprocessor = RTOLevelDataPreProcessor()
    if len(sys.argv) > 2:
        # If month and year are passed as command-line arguments
        month = sys.argv[1]
        year = sys.argv[2]
    else:
        # Use the default function to get the month and year
        month, year = get_year_month_label()
    final_df = rto_level_data_preprocessor.data_preprocessing(month, year)
    final_df.to_csv(f"rto_level_ev_data_{month}_{year}.csv", header=True, index=False)


if __name__ == "__main__":
    main()
