import glob
import os
import sys
from pathlib import Path

from azure.storage.blob import BlobServiceClient

repo_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if repo_path not in sys.path:
    sys.path.append(repo_path)

from blob_storage_utils import (
    ensure_container_exists,
    upload_globbed_files_to_container,
    upload_matching_csv_artifact,
)
from pipeline_logging import configure_pipeline_logging
from runtime_config import load_config, resolve_month_year_args

configure_pipeline_logging()


def main():
    month, year = resolve_month_year_args(sys.argv[1:])
    config = load_config()

    connection_string = config["storage"]["connection_string"]
    container_name = config["storage"]["state_wise_container_name"]
    csv_container_name = config["storage"]["state_wise_csv_container_name"]

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    csv_container_client = blob_service_client.get_container_client(csv_container_name)

    ensure_container_exists(container_client, container_name)
    ensure_container_exists(csv_container_client, csv_container_name)

    pattern = f"state_level/state_level_ev_data/*/{year}/{month}/*.xlsx"
    file_list = glob.glob(pattern)
    upload_globbed_files_to_container(
        file_list,
        relative_root="state_level/state_level_ev_data",
        container_client=container_client,
    )

    upload_matching_csv_artifact(
        processed_file_directory=str(Path.cwd()),
        csv_prefix=f"state_level_ev_data_{month}_{year}",
        csv_container_client=csv_container_client,
        csv_container_name=csv_container_name,
    )


if __name__ == "__main__":
    main()
