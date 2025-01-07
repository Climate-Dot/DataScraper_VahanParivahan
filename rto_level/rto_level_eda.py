import pandas as pd
import os

file_path = os.path.join(os.getcwd(), "rto_level", "FACT_SALES 100000-1400000.xlsx")
df = pd.read_excel(file_path)
mapping_df = pd.read_excel("Table and Mapping V2.xlsx", sheet_name="Mapping")
column_rename_map = {
"year_id": "year",
"month_name": "month",
"state_name": "state",
"Vehicle Use Type": "vehicle_use_type"
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

column_order_lst = [
'year',
'month',
'day',
'date',
'state',
'rto_name',
'rto_code',
'vehicle_class',
'vehicle_category',
'vehicle_type',
'vehicle_use_type',
'cng_only',
'diesel',
'diesel_hybrid',
'di_methyl_ether',
'dual_diesel_bio_cng',
'dual_diesel_cng',
'dual_diesel_lng',
'electric_bov',
'ethanol',
'fuel_cell_hydrogen',
'lng',
'lpg_only',
'methanol',
'not_applicable',
'petrol',
'petrol_cng',
'petrol_ethanol',
'petrol_hybrid',
'petrol_lpg',
'petrol_methanol',
'plug_in_hybrid_ev',
'pure_ev',
'solar',
'strong_hybrid_ev',
'total',
'insert_date'
]

df = pd.merge(df, mapping_df, left_on="vehicle_class", right_on="Vehicle Class", how="left")
df = df.rename(column_rename_map, axis=1)
value_replacements = {
    "Andaman & Nicobar Island": "Andaman and Nicobar",
    "UT of DNH and DD": "Dadara and Nagar Havelli",
}
df["state"] = df["state"].replace(value_replacements)
df = df.drop(["id", "state_id", "Vehicle Class", "Vehicle Type", "Vehicle Category"], axis=1)
df["date"] = pd.to_datetime(df['date']).dt.strftime('%d/%m/%Y')
df["month"] = df["month"].map(month_mapping)
df = df[column_order_lst]
df.to_csv("rto_level_historical_file2.csv", header=True, index=False)

