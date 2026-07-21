from __future__ import annotations

import csv
import logging
import os
from pathlib import Path

import pyodbc

from runtime_config import load_config
from sqlserver_utils import connect_with_retry

logger = logging.getLogger(__name__)


class BaseSqlServerIngestor:
    def __init__(
        self,
        *,
        file_prefix: str,
        staging_table_name: str,
        final_table_name: str,
        merge_key_columns: list[str],
        missing_file_hint: str,
    ) -> None:
        config = load_config()
        self.database_host = config["database"]["server"]
        self.database_name = config["database"]["database"]
        self.database_username = config["database"]["username"]
        self.database_password = config["database"]["password"]

        self.file_prefix = file_prefix
        self.staging_table_name = staging_table_name
        self.final_table_name = final_table_name
        self.merge_key_columns = list(merge_key_columns)
        self.missing_file_hint = missing_file_hint

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

    def build_file_path(self, month: str, year: str) -> str:
        return f"{self.file_prefix}_{month}_{year}.csv"

    def load_csv_rows(self, file_path: str):
        if not os.path.exists(file_path):
            raise FileNotFoundError(
                f"the file {file_path} does not exist. did you run {self.missing_file_hint} yet?"
            )

        data = []
        with open(file_path, "r", encoding="utf-8") as csv_file:
            csv_reader = csv.reader(csv_file)
            headers = next(csv_reader)
            for row in csv_reader:
                data.append(tuple(value if value != "" else None for value in row))
        return headers, data

    def build_delete_query(self) -> str:
        delete_conditions = " AND ".join(
            f"s.{column_name} = {self.final_table_name}.{column_name}"
            for column_name in self.merge_key_columns
        )
        return f"""
            DELETE FROM {self.final_table_name}
            WHERE EXISTS (
                SELECT 1 FROM {self.staging_table_name} s
                WHERE {delete_conditions}
            )
            """

    def data_ingest_from_file(self, file_path: str | Path) -> int:
        headers, data = self.load_csv_rows(str(file_path))
        if not data:
            logger.warning(
                "No rows found in %s. Skipping database ingestion for this file.",
                file_path,
            )
            return 0

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

            logger.info(
                "Deleting existing records from final table: %s", self.final_table_name
            )
            cursor.execute(self.build_delete_query())

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

        return len(data)

    def data_ingest(self, month: str, year: str) -> int:
        return self.data_ingest_from_file(self.build_file_path(month, year))
