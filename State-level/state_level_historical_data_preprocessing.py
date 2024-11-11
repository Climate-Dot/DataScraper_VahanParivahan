import pandas as pd
from pathlib import Path
from datetime import datetime


import re
import os
import warnings

warnings.filterwarnings("ignore")


# Function to extract state, month, and year
def extract_details(text):
    pattern = r"Vehicle Class Wise Fuel Data\s+of\s+([A-Za-z &]+)\s+\((\w+),(\d{4})\)"
    match = re.search(pattern, text)
    if match:
        state, month, year = match.groups()
        return state, month, year
    return None, None, None


dir_path = Path.cwd().joinpath("historical data")
file_list = os.listdir(dir_path)
final_df = pd.DataFrame()

mapping_file_path = os.path.join(
    os.getcwd(), "Table and Mapping V2.xlsx"
)
mapping_df = pd.read_excel(mapping_file_path, sheet_name="Mapping")

column_rename_map = {
            "Year": "year",
            "Month": "month",
            "Day": "day",
            "Date": "date",
            "State": "state",
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
            "FUEL CELL HYDROGEN": "fuel_cell_hydrogen",
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
            "Unnamed: 26": "total",
        }

month_mapping = {
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


def convert_date(date_str):
    return datetime.strptime(date_str, "%d/%b/%Y").strftime("%d/%m/%Y")


def remove_special_chars(text):
    return re.sub(r"\W+", " ", text)


for file in file_list:
    file_path = Path.joinpath(dir_path, f"{file}")

    # get the data for state, year and month from historical files
    meta_df = pd.read_excel(file_path, header=None)
    first_row_text = " ".join(meta_df.iloc[0, :].astype(str))
    state, month, year = extract_details(first_row_text)

    # read the original df
    df = pd.read_excel(file_path, skiprows=3, index_col=0)

    if os.path.exists(file_path):
        temp_df = pd.read_excel(file_path, skiprows=3, index_col=0)
        if temp_df.empty:
            print(
                f"No records found in report for {state}, {year}, {month}"
            )
        else:
            temp_df["Month"] = month
            temp_df["Year"] = year
            temp_df["Day"] = 1
            temp_df["Date"] = f"{1}/{month}/{year}"
            temp_df["State"] = state
            final_df = pd.concat([final_df, temp_df], ignore_index=True)

# create vehicle category and vehicle type columns
mapping_df["Vehicle Class"] = mapping_df["Vehicle Class"].apply(remove_special_chars)
mapping_df["Vehicle Class"] = mapping_df["Vehicle Class"].str.strip()
final_df = pd.merge(final_df, mapping_df, left_on="Unnamed: 1", right_on="Vehicle Class", how="left")
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
    "Andaman & Nicobar Island": "Andaman and Nicobar",
    "UT of DNH and DD": "Dadara and Nagar Havelli",
}
final_df["State"] = final_df["State"].replace(value_replacements)
# rename columns
final_df = final_df.rename(column_rename_map, errors='ignore', axis=1)
final_df["month"] = final_df["month"].map(month_mapping)
final_df["date"] = final_df["date"].apply(convert_date)
final_df["plug_in_hybrid_ev"] = final_df["plug_in_hybrid_ev"].fillna(0)
final_df["pure_ev"] = final_df["pure_ev"].fillna(0)
final_df["strong_hybrid_ev"] = final_df["strong_hybrid_ev"].fillna(0)

# reorder columns
final_df = final_df[column_rename_map.values()]
final_df.to_csv("ev_data_by_state_historical.csv", header=True, index=False)
