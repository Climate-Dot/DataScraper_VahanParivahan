from azure.identity import DefaultAzureCredential
from azure.mgmt.sql import SqlManagementClient
import logging
import time
from pipeline_logging import configure_pipeline_logging
from runtime_config import load_config

configure_pipeline_logging()
logger = logging.getLogger(__name__)


def build_sql_client():
    config = load_config()
    return (
        config,
        SqlManagementClient(DefaultAzureCredential(), config["general"]["subscription_id"]),
    )


def resume_database_if_paused():
    config, sql_client = build_sql_client()
    resource_group = config["general"]["resource_group"]
    server_name = config["database"]["server"]
    database_name = config["database"]["database"]

    db = sql_client.databases.get(resource_group, server_name, database_name)
    if db.status == "Paused":
        logger.info("Database is paused. Resuming Azure SQL database.")

        sql_client.databases.begin_resume(resource_group, server_name, database_name).wait()

        while True:
            db = sql_client.databases.get(resource_group, server_name, database_name)
            if db.status == "Online":
                logger.info("Database is now online.")
                break
            logger.info("Waiting for database to resume...")
            time.sleep(10)
    else:
        logger.info("Database is already online.")


if __name__ == "__main__":
    resume_database_if_paused()
