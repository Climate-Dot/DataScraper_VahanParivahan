import os
import re
import sys

repo_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if repo_path not in sys.path:
    sys.path.append(repo_path)

from utils import *

def validate_directory_structure(base_path, expected_states, expected_years, expected_months):
    """Check if files follow the expected structure under 'data/state_folder_name/rto_folder_name_rto_code/year/month/file.xlsx'"""

    errors = []  # Store issues

    for state in expected_states:
        state_path = os.path.join(base_path, state)
        if not os.path.exists(state_path):
            errors.append(f"Missing state folder: {state_path}")
            continue

        for rto_folder in os.listdir(state_path):
            rto_path = os.path.join(state_path, rto_folder)
            if not os.path.isdir(rto_path):
                errors.append(f"Unexpected file found in state folder: {rto_path}")
                continue

            # Ensure RTO folder follows the naming convention: "rto_folder_name_rto_code"
            if not re.match(r"^.+_[A-Z0-9]+$", rto_folder):
                errors.append(f"Invalid RTO folder naming: {rto_folder}")

            for year in expected_years:
                year_path = os.path.join(rto_path, str(year))
                if not os.path.exists(year_path):
                    errors.append(f"Missing year folder: {year_path}")
                    continue

                for month in expected_months:
                    month_path = os.path.join(year_path, month)
                    if not os.path.exists(month_path):
                        errors.append(f"Missing month folder: {month_path}")
                        continue

                    # Check for Excel files in the month folder
                    excel_files = [f for f in os.listdir(month_path) if f.endswith(".xlsx")]
                    if not excel_files:
                        errors.append(f"No Excel files found in: {month_path}")

    # Print and return errors
    if errors:
        print("\n".join(errors))
    else:
        print("✅ All files and directories are correctly structured.")

    return errors

expected_years = [2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]
expected_months = month_mapping.keys()
base_path = os.path.join(os.getcwd(), "rto_level", "rto_level_ev_data")
validate_directory_structure(base_path, state_lst, expected_years, expected_months)
