"""Uploads each fetched CSV to Azure Blob Storage as an immutable snapshot.

## Why this exists, and why it differs from V1's blob upload

V1 uploads the raw XLSX files because they were irreplaceable: once scraped, the
Vahan dashboard would not hand them back. V2's CSV is fully reproducible from the
API, so pure archival is a weak reason to keep copies.

The real reason is different: **the portal's numbers change after the fact.** The
same two RTOs' 2026 totals moved between two consecutive daily runs (16,025 →
16,121 and 25,120 → 25,338) purely from backdated registrations landing against
months already captured. The database only ever holds the latest pull, because
ingestion replaces the year wholesale — so without snapshots there is no way to
answer "what did we believe last month, and how much has it moved since?"

That makes each upload a point-in-time record, which forces two rules V1's
helpers do not follow:

1. **Never overwrite.** `blob_storage_utils.upload_*` all pass `overwrite=True`,
   which would destroy exactly the history this is meant to preserve. Uploads
   here are `overwrite=False`; an existing blob is treated as already-snapshotted
   and left alone, so re-running is idempotent rather than destructive.
2. **The blob name carries the fetch time**, not just the year — otherwise the
   several-runs-per-month cadence would collapse into one name.

Blob layout:

    year=2026/new_portal_rto_ev_data_2026__20260820T051701Z.csv

The `year=` prefix is Hive-style so the container stays queryable by tools that
understand partitioned paths, and so a year's snapshots list together.

Usage:
    python3 -m new_portal.rto_blob_snapshot --year 2026
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
from pathlib import Path

try:
    from azure.storage.blob import BlobServiceClient
except ImportError:  # pragma: no cover - azure is absent in CI, per CLAUDE.md
    BlobServiceClient = None

try:
    from azure.core.exceptions import ResourceExistsError
except ImportError:  # pragma: no cover - same
    class ResourceExistsError(Exception):
        pass

from runtime_config import load_config

from new_portal.rto_fetch import FILE_PREFIX, build_output_path

logger = logging.getLogger(__name__)

CONTAINER_CONFIG_KEY = "new_portal_rto_csv_container_name"
DEFAULT_CONTAINER_NAME = "new-portal-rto-csv"


def resolve_container_name(config: dict) -> str:
    """Prefer an explicit config key, fall back to a sensible default.

    config.yaml lives only on the VM, so requiring a new key would mean this
    breaks until someone edits it by hand. The default keeps a fresh checkout
    working while still letting the container be overridden.
    """
    return config.get("storage", {}).get(CONTAINER_CONFIG_KEY, DEFAULT_CONTAINER_NAME)


def snapshot_timestamp(csv_path: Path) -> dt.datetime:
    """When this CSV was produced, from its mtime.

    Using the file's own mtime rather than "now" makes the blob name a property
    of the artifact instead of of the upload, so re-uploading the same file is
    genuinely idempotent rather than creating a second near-identical snapshot.
    """
    return dt.datetime.fromtimestamp(csv_path.stat().st_mtime, tz=dt.timezone.utc)


def build_snapshot_blob_name(year: int, fetched_at: dt.datetime) -> str:
    stamp = fetched_at.astimezone(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"year={year}/{FILE_PREFIX}_{year}__{stamp}.csv"


def upload_snapshot(
    csv_path: Path,
    year: int,
    *,
    container_client=None,
    fetched_at: dt.datetime | None = None,
) -> tuple[str, bool]:
    """Upload one CSV as an immutable snapshot.

    Returns (blob_name, uploaded) — `uploaded` is False when a snapshot with
    that exact name already existed, which is not an error.
    """
    if not csv_path.exists():
        raise FileNotFoundError(
            f"{csv_path} does not exist. Did new_portal.rto_fetch run?"
        )

    fetched_at = fetched_at or snapshot_timestamp(csv_path)
    blob_name = build_snapshot_blob_name(year, fetched_at)

    if container_client is None:
        config = load_config()
        container_name = resolve_container_name(config)
        service = BlobServiceClient.from_connection_string(
            config["storage"]["connection_string"]
        )
        container_client = service.get_container_client(container_name)
        try:
            container_client.create_container()
            logger.info("Created container %s", container_name)
        except ResourceExistsError:
            pass

    blob_client = container_client.get_blob_client(blob_name)
    with csv_path.open("rb") as handle:
        try:
            blob_client.upload_blob(handle, overwrite=False)
        except ResourceExistsError:
            # Same artifact, already snapshotted. Leaving it untouched is the
            # whole point — overwriting would destroy the history.
            logger.info("Snapshot %s already exists; leaving it as-is", blob_name)
            return blob_name, False

    logger.info(
        "Uploaded snapshot %s (%s bytes)", blob_name, csv_path.stat().st_size
    )
    return blob_name, True


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, required=True, help="Year whose CSV to snapshot.")
    parser.add_argument("--csv-path", type=Path, help="Override the CSV location.")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    csv_path = args.csv_path or build_output_path(args.year)
    blob_name, uploaded = upload_snapshot(csv_path, args.year)
    logger.info("%s -> %s", "Uploaded" if uploaded else "Already present", blob_name)


if __name__ == "__main__":
    main()
