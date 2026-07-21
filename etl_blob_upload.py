from __future__ import annotations

import glob
from pathlib import Path

from azure.storage.blob import BlobServiceClient

from blob_storage_utils import (
    ensure_container_exists,
    upload_globbed_files_to_container,
    upload_matching_csv_artifact,
)
from runtime_config import load_config


def upload_pipeline_artifacts(
    *,
    month: str,
    year: str,
    raw_file_pattern: str,
    relative_root: str,
    raw_container_config_key: str,
    csv_container_config_key: str,
    csv_prefix: str,
    processed_file_directory: str | Path | None = None,
):
    config = load_config()

    connection_string = config["storage"]["connection_string"]
    container_name = config["storage"][raw_container_config_key]
    csv_container_name = config["storage"][csv_container_config_key]

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    csv_container_client = blob_service_client.get_container_client(csv_container_name)

    ensure_container_exists(container_client, container_name)
    ensure_container_exists(csv_container_client, csv_container_name)

    file_list = glob.glob(raw_file_pattern.format(month=month, year=year))
    uploaded_raw_files = upload_globbed_files_to_container(
        file_list,
        relative_root=relative_root,
        container_client=container_client,
    )

    uploaded_csv_name = upload_matching_csv_artifact(
        processed_file_directory=str(processed_file_directory or Path.cwd()),
        csv_prefix=csv_prefix.format(month=month, year=year),
        csv_container_client=csv_container_client,
        csv_container_name=csv_container_name,
    )

    return uploaded_raw_files, uploaded_csv_name
