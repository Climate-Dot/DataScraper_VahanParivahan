import os
import pandas as pd
import re
import warnings

warnings.filterwarnings("ignore")

columns = [
    "year",
    "month",
    "day",
    "date",
    "state",
    "vehicle_class",
    "vehicle_type",
    "vehicle_category",
    "maker",
    "cng_only",
    "diesel",
    "diesel_hybrid",
    "di_methyl_ether",
    "dual_diesel_bio_cng",
    "dual_diesel_cng",
    "dual_diesel_lng",
    "electric_bov",
    "ethanol",
    "fuel_cell_hydrogen",
    "lng",
    "lpg_only",
    "methanol",
    "not_applicable",
    "petrol",
    "petrol_cng",
    "petrol_ethanol",
    "petrol_hybrid",
    "petrol_lng",
    "petrol_methanol",
    "plug_in_hybrid_ev",
    "pure_ev",
    "solar",
    "strong_hybrid_ev",
    "total",
]
file_path = os.path.join(os.getcwd(), "OEM-level", "Table and Mapping.xlsx")
mapping_df = pd.read_excel(file_path, sheet_name="Mapping")

df = pd.DataFrame()
parent_directory = os.path.join(
    os.getcwd(), "OEM-level", "oem_data_by_state_and_category"
)
for state in os.listdir(parent_directory):
    state_path = os.path.join(parent_directory, state)
    for vehicle_class in os.listdir(state_path):
        vehicle_class_path = os.path.join(state_path, vehicle_class)
        for year in os.listdir(vehicle_class_path):
            year_path = os.path.join(vehicle_class_path, year)
            for month in os.listdir(year_path):
                month_path = os.path.join(year_path, month)
                file_path = os.path.join(month_path, "reportTable.xlsx")
                temp_df = pd.read_excel(file_path, skiprows=3, index_col=0)
                if temp_df.empty:
                    print(
                        f"No records found in report for {vehicle_class}, {state}, {year}, {month}"
                    )
                else:
                    # print(temp_df.head(10))
                    # print(temp_df.columns)
                    temp_df["Month"] = month
                    temp_df["Year"] = year
                    temp_df["Day"] = 1
                    temp_df["Date"] = f"{1}/{month}/{year}"
                    temp_df["Vehicle Class"] = vehicle_class
                    temp_df["State"] = state
                    df = df._append(temp_df)


def remove_special_chars(text):
    return re.sub(r"\W+", " ", text)


mapping_df["Vehicle Class"] = mapping_df["Vehicle Class"].apply(remove_special_chars)
# create vehicle category and vehicle type columns
df = pd.merge(df, mapping_df, on="Vehicle Class", how="left")
# rename columns
df = df.rename({"Unnamed: 1": "Maker", "Unnamed: 26": "Total"}, axis=1)
# reorder columns
df = df[columns]
df.to_csv("oem_data_by_state_and_category_2024_to_2013.csv", header=True, index=False)
