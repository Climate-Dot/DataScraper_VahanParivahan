from azure.identity import DefaultAzureCredential
from azure.mgmt.sql import SqlManagementClient
import time
import yaml

with open("config.yaml", "r") as file:
    config = yaml.safe_load(file)

# Set your Azure configuration
subscription_id = config["general"]["subscription_id"]
resource_group = config["general"]["resource_group"]
server_name = config["database"]["server"]
database_name = config["database"]["database"]

# Authenticate and create the client
credential = DefaultAzureCredential()
sql_client = SqlManagementClient(credential, subscription_id)


def resume_database_if_paused():
    # Check database status
    db = sql_client.databases.get(resource_group, server_name, database_name)
    if db.status == "Paused":
        print("Database is paused. Resuming...")

        # Resume the database
        sql_client.databases.begin_resume(resource_group, server_name, database_name).wait()

        # Wait for the database to fully resume
        while True:
            db = sql_client.databases.get(resource_group, server_name, database_name)
            if db.status == "Online":
                print("Database is now online.")
                break
            print("Waiting for database to resume...")
            time.sleep(10)
    else:
        print("Database is already online.")


# Main script
if __name__ == "__main__":
    resume_database_if_paused()
