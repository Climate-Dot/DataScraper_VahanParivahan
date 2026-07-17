import glob
import os
import sys

from azure.storage.blob import BlobServiceClient

repo_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if repo_path not in sys.path:
    sys.path.append(repo_path)

from blob_storage_utils import ensure_container_exists, upload_globbed_files_to_container
from pipeline_logging import configure_pipeline_logging
from runtime_config import load_config
from utils import month_mapping

configure_pipeline_logging()


def main():
    config = load_config()

    connection_string = config["storage"]["connection_string"]
    container_name = config["storage"]["rto_wise_container_name"]

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)

    ensure_container_exists(container_client, container_name)

    years = [2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]
    for year in years:
        for month in month_mapping.keys():
            pattern = f"rto_level/rto_level_ev_data/*/*/{year}/{month}/*.xlsx"
            file_list = glob.glob(pattern)
            upload_globbed_files_to_container(
                file_list,
                relative_root="rto_level/rto_level_ev_data",
                container_client=container_client,
            )


if __name__ == "__main__":
    main()
