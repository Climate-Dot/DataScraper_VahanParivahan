import os
import sys
import yaml
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient

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
container_name = config["storage"]["container_name"]
csv_container_name = config["storage"]["csv_container_name"]

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
local_directory = f"OEM-level/oem_data_by_state_and_category/{year}/{month}"
processed_file_directory = os.getcwd()
# Walk through the directory structure and upload each file
for root, dirs, files in os.walk(local_directory):
    for file in files:
        if file.endswith(".xlsx"):
            file_path = os.path.join(root, file)
            # Blob path matches the directory structure under the local directory
            blob_path = os.path.relpath(file_path, local_directory).replace("\\", "/")
            blob_client = container_client.get_blob_client(blob=blob_path)

            # Upload the file
            with open(file_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
                print(f"Uploaded {file_path} to {blob_path}")

# Check if the CSV file exists and upload it to climatedotraw
csv_file_pattern = f"oem_data_by_state_and_category_{year}_{month}"
for root, dirs, files in os.walk(processed_file_directory):
    for file in files:
        if file.startswith(csv_file_pattern) and file.endswith(".csv"):
            csv_file_path = os.path.join(root, file)
            # Use only the CSV filename for the blob path
            blob_client = csv_container_client.get_blob_client(blob=file)

            # Upload the CSV file
            with open(csv_file_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
                print(f"Uploaded {csv_file_path} to {csv_container_name}/{file}")