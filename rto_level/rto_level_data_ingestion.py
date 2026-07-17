import csv
import logging
import os
import pyodbc
import sys

repo_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if repo_path not in sys.path:
    sys.path.append(repo_path)

from pipeline_logging import configure_pipeline_logging
from runtime_config import load_config, resolve_month_year_args
from sqlserver_utils import connect_with_retry

configure_pipeline_logging()
logger = logging.getLogger(__name__)


class RtoDataIngest:
    def __init__(self):
        config = load_config()
        self.database_host = config["database"]["server"]
        self.database_name = config["database"]["database"]
        self.database_username = config["database"]["username"]
        self.database_password = config["database"]["password"]

        self.staging_table_name = "staging_fact_ev_data_by_rto"
        self.final_table_name = "fact_ev_data_by_rto"
        self.driver = "{ODBC Driver 18 for SQL Server}"
        self.sql_attr_connection_timeout = 113
        self.login_timeout = 30
        self.connection_timeout = 30

    def connect(self):
        return connect_with_retry(
            lambda: pyodbc.connect(
                driver=self.driver,
                server=self.database_host,
                database=self.database_name,
                uid=self.database_username,
                pwd=self.database_password,
                timeout=self.login_timeout,
                attrs_before={
                    self.sql_attr_connection_timeout: self.connection_timeout
                },
            ),
            logger=logger,
            connection_error_types=(pyodbc.Error,),
        )

    def data_ingest(self, month, year):
        file_path = f"rto_level_ev_data_{month}_{year}.csv"
        if not os.path.exists(file_path):
            raise FileNotFoundError(
                f"the file {file_path} does not exist. did you run data_pre_processing yet?"
            )

        data = []
        with open(file_path, "r", encoding="utf-8") as csv_file:
            csv_reader = csv.reader(csv_file)
            headers = next(csv_reader)
            for row in csv_reader:
                data.append(tuple(value if value != "" else None for value in row))

        logger.info("Connecting to the database...")
        conn = self.connect()
        cursor = conn.cursor()

        try:
            logger.info("Truncating staging table: %s", self.staging_table_name)
            cursor.execute(f"TRUNCATE TABLE {self.staging_table_name}")

            columns = ", ".join(headers)
            placeholders = ", ".join(["?" for _ in headers])
            insert_query = (
                f"INSERT INTO {self.staging_table_name} "
                f"({columns}, inserted_at) VALUES ({placeholders}, GETDATE())"
            )

            logger.info("Inserting data into staging table: %s", self.staging_table_name)
            cursor.fast_executemany = True
            cursor.executemany(insert_query, data)
            conn.commit()

            delete_query = f"""
            DELETE FROM {self.final_table_name}
            WHERE EXISTS (
                SELECT 1 FROM {self.staging_table_name} s
                WHERE s.date = {self.final_table_name}.date
                AND s.state = {self.final_table_name}.state
                AND s.rto_code = {self.final_table_name}.rto_code
                AND s.rto_name = {self.final_table_name}.rto_name
                AND s.vehicle_class = {self.final_table_name}.vehicle_class
            )
            """
            logger.info(
                "Deleting existing records from final table: %s", self.final_table_name
            )
            cursor.execute(delete_query)

            transfer_query = f"""
            INSERT INTO {self.final_table_name}
            SELECT * FROM {self.staging_table_name}
            """
            logger.info(
                "Transferring data from staging table to final table: %s",
                self.final_table_name,
            )
            cursor.execute(transfer_query)
            conn.commit()
        finally:
            cursor.close()
            conn.close()


def main():
    rto_ev_data_ingest = RtoDataIngest()
    month, year = resolve_month_year_args(sys.argv[1:])
    rto_ev_data_ingest.data_ingest(month, year)


if __name__ == "__main__":
    main()
