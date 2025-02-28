import os
import glob
import logging
import sys
import shutil
import yaml
from azure.storage.blob import BlobServiceClient

logging.basicConfig(
    level=logging.INFO,  # Set the logging level (INFO, DEBUG, etc.)
    format="%(asctime)s - %(levelname)s - %(message)s",  # log message format
)

repo_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if repo_path not in sys.path:
    sys.path.append(repo_path)

from utils import *

if len(sys.argv) > 2:
    # If month and year are passed as command-line arguments
    month = sys.argv[1]
    year = sys.argv[2]
else:
    # Use the default function to get the month and year
    month, year = get_year_month_label()

# Load configuration from config.yaml
with open("config.yaml", "r") as config_file:
    config = yaml.safe_load(config_file)

# Extract values from the YAML config
connection_string = config["storage"]["connection_string"]
container_name = config["storage"]["rto_wise_container_name"]
csv_container_name = config["storage"]["rto_wise_csv_container_name"]

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
pattern = f"rto_level/rto_level_ev_data/*/*/{year}/{month}/*.xlsx"
file_list = glob.glob(pattern)
processed_file_directory = os.getcwd()

for file_path in file_list:
    relative_path = os.path.relpath(file_path, "rto_level/rto_level_ev_data").replace(
        "\\", "/"
    )
    blob_client = container_client.get_blob_client(blob=relative_path)
    # Upload the file
    with open(file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
        print(f"Uploaded {file_path} to {file_path}")

    # remove directory and file from structure
    shutil.rmtree(os.path.dirname(file_path))

# Check if the CSV file exists and upload it to climatedotraw
csv_file_pattern = f"rto_level_ev_data_{month}_{year}"
csv_file = next(
    (
        file
        for file in os.listdir(processed_file_directory)
        if file.startswith(csv_file_pattern) and file.endswith(".csv")
    ),
    None,
)
if csv_file:
    csv_file_path = os.path.join(processed_file_directory, csv_file)
    # Use only the CSV filename for the blob path
    blob_client = csv_container_client.get_blob_client(blob=csv_file)

    # Upload the CSV file
    with open(csv_file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
        print(f"Uploaded {csv_file_path} to {csv_container_name}/{csv_file}")
    # remove file
    os.remove(csv_file)
