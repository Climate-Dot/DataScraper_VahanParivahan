import logging

from azure.core.exceptions import ResourceExistsError


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
