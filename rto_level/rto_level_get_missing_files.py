import json
import logging
import os
import sys

from concurrent.futures import ThreadPoolExecutor, as_completed

# configure logging to write to both the console and a file
logging.basicConfig(
    level=logging.INFO,  # Set the logging level (INFO, DEBUG, etc.)
    format='%(asctime)s - %(levelname)s - %(message)s',  # log message format
)

repo_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if repo_path not in sys.path:
    sys.path.append(repo_path)

from utils import *
from rto_level.rto_level_data_scraper import RTODataScraper


month, year = get_year_month_label()

rto_data_scraper = RTODataScraper()

with open("output.json", "r") as f:
    state_rto_mapping = json.load(f)

parameters = []
for state in state_lst:
    all_rto_office_names = state_rto_mapping.get(state)
    for rto_office_name in all_rto_office_names:
        directory_path = os.path.join(
            os.getcwd(),
            "rto_level",
            "rto_level_ev_data",
            re.sub(r"[^a-zA-Z\s]", " ", state).rstrip(),
            rto_data_scraper.extract_rto_name_and_code(rto_office_name),
            str(year),
            month,
            "reportTable.xlsx"
        )

        if not os.path.exists(directory_path):
            parameters.append((state, rto_office_name, year, month))

logging.info(f"There are {len(parameters)} missing files..beginning to extract")

# run selenium function in parallel
with ThreadPoolExecutor(
        max_workers=10
) as executor:  # adjust max_workers based on your system's capability
    futures = [
        executor.submit(rto_data_scraper.run_selenium, args)
        for args in parameters
    ]

    for future in as_completed(futures):
        try:
            result = future.result()
        except Exception as e:
            logging.info(f"Exception occurred: {e}")


file_count = 0
directory = os.path.join(os.getcwd(), "rto_level_ev_data")
for root, _, files in os.walk(directory):
    for file in files:
        if file.endswith(".xlsx"):
            file_count += 1

expected_file_count = sum(item for item in state_rto_mapping.values())
assert expected_file_count == file_count

