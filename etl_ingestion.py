from __future__ import annotations

import csv
import logging
import os
from pathlib import Path

try:
    import pyodbc
except ImportError:  # pragma: no cover - pyodbc needs system ODBC libs absent in CI/tests
    # Real runs on the VM have pyodbc installed; unit tests patch the connection.
    pyodbc = None

from runtime_config import load_config
from sqlserver_utils import connect_with_retry

logger = logging.getLogger(__name__)

# Rows per executemany call when loading the staging table.
#
# pyodbc's fast_executemany builds ONE parameter array for the whole call, so a
# wide table plus a large load multiplies out fast: the first nationwide
# new-portal pull was 144,182 rows x 51 columns = 7.35 million parameters in a
# single call, and it hung — 34 minutes, 4 seconds of CPU, no rows landed, the
# server sitting idle waiting on a client that never sent anything more.
#
# V1's monthly loads (~16k rows) never came near this, which is why it went
# unnoticed. Chunking costs a few extra round trips on those and makes large
# loads finish at all. The per-chunk log line matters too: without it a stalled
# load is indistinguishable from a slow one, which is exactly what made the
# original failure so expensive to diagnose.
INSERT_CHUNK_SIZE = 5_000

# Opt-in fast path, for loads big enough that per-row round trips dominate.
#
# Measured 2026-09-24 against the production Azure SQL (GP_S_Gen5_1, serverless,
# 1 vCore) from the VM, loading the 50-column V2 table:
#
#   executemany, fast_executemany=False   4.6 rows/s
#   executemany, fast_executemany=True   17.2 rows/s   <- 144k rows = 4.6 hours
#   multi-row INSERT ... VALUES          74.0 rows/s   <- 144k rows = 33 minutes
#
# The database was idle throughout (0% CPU, 0% IO, 0% log) — the cost is
# entirely network round trips, and fast_executemany was still paying roughly
# one per row. Packing many rows into a single statement is what removes them.
#
# Off by default: the V1 pipelines load ~16k rows a month on a path that has
# worked for years, and this is not worth changing under them without need.
MAX_STATEMENT_PARAMETERS = 2100
PARAMETER_HEADROOM = 10


def rows_per_multirow_statement(column_count: int) -> int:
    """How many rows fit in one INSERT ... VALUES under the parameter ceiling.

    SQL Server rejects a statement with more than 2100 parameters, and hitting
    exactly 2100 still fails, so this leaves headroom. With the V2 table's 50
    columns that is 41 rows per statement.
    """
    return max(1, (MAX_STATEMENT_PARAMETERS - PARAMETER_HEADROOM) // column_count)


class BaseSqlServerIngestor:
    def __init__(
        self,
        *,
        file_prefix: str,
        staging_table_name: str,
        final_table_name: str,
        merge_key_columns: list[str],
        missing_file_hint: str,
        use_multirow_insert: bool = False,
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
        self.use_multirow_insert = use_multirow_insert

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

    def build_delete_query(
        self,
        replacement_scope_columns: list[str] | None = None,
    ) -> str:
        scope_columns = replacement_scope_columns or self.merge_key_columns
        delete_conditions = " AND ".join(
            f"s.{column_name} = {self.final_table_name}.{column_name}"
            for column_name in scope_columns
        )
        return f"""
            DELETE FROM {self.final_table_name}
            WHERE EXISTS (
                SELECT 1 FROM {self.staging_table_name} s
                WHERE {delete_conditions}
            )
            """

    def data_ingest_from_file(
        self,
        file_path: str | Path,
        *,
        replacement_scope_columns: list[str] | None = None,
    ) -> int:
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

            logger.info(
                "Inserting %s rows into staging table: %s",
                len(data), self.staging_table_name,
            )
            if self.use_multirow_insert:
                self._load_staging_multirow(cursor, headers, data)
            else:
                cursor.fast_executemany = True
                for start in range(0, len(data), INSERT_CHUNK_SIZE):
                    chunk = data[start : start + INSERT_CHUNK_SIZE]
                    cursor.executemany(insert_query, chunk)
                    logger.info(
                        "  staged %s/%s rows",
                        min(start + len(chunk), len(data)), len(data),
                    )
            conn.commit()

            logger.info(
                "Deleting existing records from final table: %s", self.final_table_name
            )
            cursor.execute(self.build_delete_query(replacement_scope_columns))

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

    def _load_staging_multirow(self, cursor, headers: list[str], data: list) -> None:
        """Load staging with multi-row INSERT ... VALUES statements.

        One statement carries as many rows as the parameter ceiling allows, so a
        144k-row load costs ~3.5k round trips instead of 144k. `inserted_at` is
        GETDATE() per row, matching the executemany path exactly.
        """
        columns = ", ".join(headers)
        row_placeholder = "(" + ", ".join("?" for _ in headers) + ", GETDATE())"
        per_statement = rows_per_multirow_statement(len(headers))
        logger.info(
            "  multi-row insert: %s columns, %s rows per statement",
            len(headers), per_statement,
        )

        for start in range(0, len(data), per_statement):
            chunk = data[start : start + per_statement]
            sql = (
                f"INSERT INTO {self.staging_table_name} ({columns}, inserted_at) "
                f"VALUES " + ", ".join([row_placeholder] * len(chunk))
            )
            cursor.execute(sql, [value for row in chunk for value in row])
            done = start + len(chunk)
            if done % (per_statement * 20) < per_statement or done == len(data):
                logger.info("  staged %s/%s rows", done, len(data))

    def data_ingest(self, month: str, year: str) -> int:
        return self.data_ingest_from_file(
            self.build_file_path(month, year),
            replacement_scope_columns=["date"],
        )
