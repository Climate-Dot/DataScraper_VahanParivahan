"""Loads a year into SQL Server by reading its CSV straight out of blob storage.

## Why this exists alongside rto_ingest

`rto_ingest` pushes rows up from the VM as parameterised INSERTs. That is
network-bound and slow at this scale: a 170,718-row year took ~36 minutes with
the database sitting at 0% CPU, 0% IO the entire time. The cost is round trips,
not work — even with multi-row statements packing 41 rows each, a year is
thousands of round trips against a server that answers in milliseconds.

Azure SQL can read the CSV directly from Azure Blob Storage, where this pipeline
already uploads every fetched file for archival. Measured 2026-09-29 on the same
2022 file: **16.5 seconds**, roughly 130x faster, because nothing crosses the
wire except the instruction to go and read it.

## Shape of the load

    blob CSV --BULK INSERT--> landing --INSERT SELECT--> staging --> final

The landing table exists because BULK INSERT maps columns positionally and
staging carries an `inserted_at` column the CSV does not. Landing is shaped
exactly like the file; the timestamp is added server-side on the way to staging.

Empty CSV fields become NULL automatically under `FORMAT='CSV'`, which is
exactly the semantics the row-by-row path produces by hand — verified on the
2022 file, where 59,520 blank fuel cells arrived as NULL rather than 0. That
matters: a fabricated 0 would be indistinguishable from a real count of nil.

## Credentials

The database-scoped credential holds a short-lived SAS, regenerated on every
run rather than stored. Nothing long-lived is written to the database, and a
leaked credential expires within hours.

Usage:
    python3 -m new_portal.rto_bulk_ingest --year 2025
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging

try:
    from azure.storage.blob import (
        BlobServiceClient,
        ContainerSasPermissions,
        generate_container_sas,
    )
except ImportError:  # pragma: no cover - azure is absent in CI, per CLAUDE.md
    BlobServiceClient = None
    ContainerSasPermissions = None
    generate_container_sas = None

try:
    import pyodbc
except ImportError:  # pragma: no cover - same
    pyodbc = None

from runtime_config import load_config

from new_portal.rto_blob_snapshot import FILE_PREFIX, resolve_container_name
from new_portal.rto_ingest import FINAL_TABLE, REPLACEMENT_SCOPE_COLUMNS, STAGING_TABLE
from new_portal.schema import CSV_COLUMNS

logger = logging.getLogger(__name__)

LANDING_TABLE = "landing_fact_ev_data_by_rto_v2"
CREDENTIAL_NAME = "np_blob_cred"
DATA_SOURCE_NAME = "np_blob"

# Long enough for a load that takes seconds, short enough that a leaked token is
# worthless by the time anyone finds it.
SAS_LIFETIME_HOURS = 2


def latest_snapshot_blob(container_client, year: int) -> str:
    """The most recent snapshot for a year.

    Snapshots are immutable and timestamped, so a year accumulates one blob per
    run. The newest is the one that matches the CSV just written; sorting works
    because the timestamp is fixed-width and UTC.
    """
    prefix = f"year={year}/"
    names = sorted(
        b.name for b in container_client.list_blobs(name_starts_with=prefix)
        if b.name.endswith(".csv") and FILE_PREFIX in b.name
    )
    if not names:
        raise FileNotFoundError(
            f"No snapshot under {prefix} in the container. "
            f"Run new_portal.rto_blob_snapshot --year {year} first."
        )
    return names[-1]


def build_sas(config: dict, container: str) -> tuple[str, str]:
    """Returns (account_name, sas_token) for read access to the container."""
    service = BlobServiceClient.from_connection_string(
        config["storage"]["connection_string"]
    )
    token = generate_container_sas(
        account_name=service.account_name,
        container_name=container,
        account_key=service.credential.account_key,
        permission=ContainerSasPermissions(read=True, list=True),
        expiry=dt.datetime.now(dt.timezone.utc)
        + dt.timedelta(hours=SAS_LIFETIME_HOURS),
    )
    return service.account_name, token


def refresh_external_data_source(cursor, account: str, container: str, sas: str) -> None:
    """(Re)point the external data source at the container with a fresh SAS.

    Dropped and recreated every run: the SAS is short-lived by design, so a
    stale credential would fail in a way that looks like a permissions problem
    rather than an expiry.
    """
    cursor.execute(
        f"IF EXISTS (SELECT 1 FROM sys.external_data_sources WHERE name='{DATA_SOURCE_NAME}') "
        f"DROP EXTERNAL DATA SOURCE {DATA_SOURCE_NAME}"
    )
    cursor.execute(
        f"IF EXISTS (SELECT 1 FROM sys.database_scoped_credentials WHERE name='{CREDENTIAL_NAME}') "
        f"DROP DATABASE SCOPED CREDENTIAL {CREDENTIAL_NAME}"
    )
    cursor.execute(
        f"CREATE DATABASE SCOPED CREDENTIAL {CREDENTIAL_NAME} "
        f"WITH IDENTITY='SHARED ACCESS SIGNATURE', SECRET=?".replace("?", f"'{sas}'")
    )
    cursor.execute(
        f"CREATE EXTERNAL DATA SOURCE {DATA_SOURCE_NAME} WITH ("
        f"TYPE=BLOB_STORAGE, "
        f"LOCATION='https://{account}.blob.core.windows.net/{container}', "
        f"CREDENTIAL={CREDENTIAL_NAME})"
    )


def build_bulk_insert(blob_name: str) -> str:
    """BULK INSERT for one snapshot.

    ROWTERMINATOR is CRLF because csv.writer defaults to it, CODEPAGE 65001
    because the CSV is UTF-8, and FIRSTROW=2 skips the header. Getting any of
    these wrong silently mangles rows rather than failing, so they are pinned
    here rather than left to defaults.
    """
    return (
        f"BULK INSERT dbo.{LANDING_TABLE} FROM '{blob_name}' "
        f"WITH (DATA_SOURCE='{DATA_SOURCE_NAME}', FORMAT='CSV', FIELDQUOTE='\"', "
        f"FIRSTROW=2, ROWTERMINATOR='0x0d0a', CODEPAGE='65001')"
    )


def build_delete_query() -> str:
    """Delete the rows this load replaces, scoped the same way rto_ingest is."""
    conditions = " AND ".join(
        f"s.{c} = {FINAL_TABLE}.{c}" for c in REPLACEMENT_SCOPE_COLUMNS
    )
    return (
        f"DELETE FROM {FINAL_TABLE} WHERE EXISTS "
        f"(SELECT 1 FROM {STAGING_TABLE} s WHERE {conditions})"
    )


def bulk_ingest(year: int, *, connection=None, config: dict | None = None) -> int:
    config = config or load_config()
    container = resolve_container_name(config)
    account, sas = build_sas(config, container)

    service = BlobServiceClient.from_connection_string(
        config["storage"]["connection_string"]
    )
    blob = latest_snapshot_blob(service.get_container_client(container), year)
    logger.info("Loading %s from container %s", blob, container)

    if connection is None:
        db = config["database"]
        connection = pyodbc.connect(
            "DRIVER={ODBC Driver 18 for SQL Server};"
            f"SERVER={db['server']};DATABASE={db['database']};"
            f"UID={db['username']};PWD={db['password']};"
            "Encrypt=yes;TrustServerCertificate=yes",
            autocommit=True,
        )
    cursor = connection.cursor()

    refresh_external_data_source(cursor, account, container, sas)

    cursor.execute(f"TRUNCATE TABLE dbo.{LANDING_TABLE}")
    cursor.execute(build_bulk_insert(blob))
    cursor.execute(f"SELECT COUNT(*) FROM dbo.{LANDING_TABLE}")
    landed = cursor.fetchone()[0]
    logger.info("Bulk-loaded %s rows into %s", f"{landed:,}", LANDING_TABLE)
    if not landed:
        raise RuntimeError(f"{blob} produced no rows; refusing to replace {year}")

    columns = ", ".join(CSV_COLUMNS)
    cursor.execute(f"TRUNCATE TABLE {STAGING_TABLE}")
    cursor.execute(
        f"INSERT INTO {STAGING_TABLE} ({columns}, inserted_at) "
        f"SELECT {columns}, GETDATE() FROM dbo.{LANDING_TABLE}"
    )

    cursor.execute(build_delete_query())
    cursor.execute(f"INSERT INTO {FINAL_TABLE} SELECT * FROM {STAGING_TABLE}")
    logger.info("Ingested %s rows into %s for %s", f"{landed:,}", FINAL_TABLE, year)
    return landed


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, required=True)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    rows = bulk_ingest(args.year)
    if not rows:
        raise SystemExit(f"No rows ingested for {args.year}")


if __name__ == "__main__":
    main()
