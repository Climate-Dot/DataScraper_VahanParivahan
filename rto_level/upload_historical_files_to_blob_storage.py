import glob
import logging
import os
import sys
import shutil
import yaml

from azure.storage.blob import BlobServiceClient

# configure logging to write to both the console and a file
logging.basicConfig(
    level=logging.INFO,  # Set the logging level (INFO, DEBUG, etc.)
    format="%(asctime)s - %(levelname)s - %(message)s",  # log message format
)

repo_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if repo_path not in sys.path:
    sys.path.append(repo_path)

from utils import *


with open("config.yaml", "r") as config_file:
    config = yaml.safe_load(config_file)

# Extract values from the YAML config
connection_string = config["storage"]["connection_string"]
container_name = config["storage"]["state_wise_container_name"]
csv_container_name = config["storage"]["state_wise_csv_container_name"]

# Create the BlobServiceClient
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# Initialize the container client
container_client = blob_service_client.get_container_client(container_name)
csv_container_client = blob_service_client.get_container_client(csv_container_name)

# Ensure the container exists
try:
    container_client.create_container()
except Exception as e:
    print(f"Container may already exist: {e}")

# Ensure processed data container exists
try:
    csv_container_client.create_container()
except Exception as e:
    print(f"Container may already exist: {e}")

# Local directory to scan
years = [2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]
for year in years:
    for month in month_mapping.keys():
        pattern = f"rto_level/rto_level_ev_data/*/*/{year}/{month}/*.xlsx"
        file_list = glob.glob(pattern)
        processed_file_directory = os.getcwd()

        for file_path in file_list:
            relative_path = os.path.relpath(
                file_path, "rto_level/rto_level_ev_data"
            ).replace("\\", "/")
            print(relative_path)
            blob_client = container_client.get_blob_client(blob=relative_path)
            # Upload the file
            with open(file_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
                print(f"Uploaded {file_path} to {file_path}")

            # remove directory and file from structure
            shutil.rmtree(os.path.dirname(file_path))
