import logging
import os
import shutil

try:
    from azure.core.exceptions import ResourceExistsError
except ImportError:  # pragma: no cover - fallback for local unit tests without azure libs
    class ResourceExistsError(Exception):
        pass


def ensure_container_exists(container_client, container_name):
    """Create a blob container if needed and keep expected exists-cases quiet."""
    try:
        container_client.create_container()
        logging.info("Created blob container: %s", container_name)
        return True
    except ResourceExistsError:
        logging.info("Blob container already exists: %s", container_name)
        return False
    except Exception as exc:
        error_code = getattr(exc, "error_code", None)
        if error_code == "ContainerAlreadyExists" or "ContainerAlreadyExists" in str(
            exc
        ):
            logging.info("Blob container already exists: %s", container_name)
            return False

        logging.error(
            "Failed to ensure blob container exists container=%s error=%s",
            container_name,
            exc,
        )
        raise


def upload_globbed_files_to_container(file_paths, relative_root, container_client):
    uploaded_files = 0
    for file_path in file_paths:
        relative_path = os.path.relpath(file_path, relative_root).replace("\\", "/")
        blob_client = container_client.get_blob_client(blob=relative_path)
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        logging.info("Uploaded %s to blob path %s", file_path, relative_path)
        directory_to_remove = os.path.dirname(file_path)
        if os.path.isdir(directory_to_remove):
            shutil.rmtree(directory_to_remove)
        uploaded_files += 1

    return uploaded_files


def upload_matching_csv_artifact(
    *,
    processed_file_directory,
    csv_prefix,
    csv_container_client,
    csv_container_name,
):
    csv_file = next(
        (
            file
            for file in os.listdir(processed_file_directory)
            if file.startswith(csv_prefix) and file.endswith(".csv")
        ),
        None,
    )

    if not csv_file:
        return None

    csv_file_path = os.path.join(processed_file_directory, csv_file)
    blob_client = csv_container_client.get_blob_client(blob=csv_file)

    with open(csv_file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

    logging.info("Uploaded %s to %s/%s", csv_file_path, csv_container_name, csv_file)
    os.remove(csv_file_path)
    return csv_file
