#!/usr/bin/env python3
"""Off-VM dead-man's switch: has a V2 run landed a snapshot recently?

Every other check in this pipeline runs on the VM, which means none of them can
notice the one failure that actually happened in November 2026: the VM never
powered on, so no cron job fired, so nothing alerted and silence was
indistinguishable from success. This check runs on a GitHub-hosted runner, where
the VM's power state cannot reach it.

**Why blob age and not the database.** A GitHub runner can reach Azure Blob
Storage with a connection string. Whether it can reach the Azure SQL server
depends on that server's firewall allow-list, which is not represented in this
repo and was never confirmed. Snapshot age is a sound proxy either way, because
``new_portal_rto_monthly.sh`` snapshots *before* it ingests: a fresh snapshot
proves a run happened and completed its fetch, and its absence proves one did
not. If SQL turns out to be reachable, an off-VM ``MAX([inserted_at])`` watchdog
would be strictly stronger and should replace this.

**The container name is a code default, not a configured value.** Verified
2026-11-03: the VM's ``config.yaml`` has no ``new_portal`` storage key at all, so
production runs on ``rto_blob_snapshot.DEFAULT_CONTAINER_NAME``. This module
imports that constant rather than re-spelling the string, so if the default moves
the watchdog moves with it. What it cannot follow is somebody *adding*
``storage.new_portal_rto_csv_container_name`` to ``config.yaml``: the pipeline
would start writing somewhere else and this watchdog would keep reading an
abandoned container that never goes stale in the way it is watching for -- a
watchdog reporting healthy forever. If that key is ever added, this file must be
told about it on the same day.

Dependencies are deliberately thin -- ``azure-storage-blob`` for the client and
``requests`` only because importing the container name reaches
``new_portal.rto_fetch``. It must never read ``config.yaml``, which does not
exist off the VM. The alert goes out through ``ops/send_chat_alert.py``, which is
stdlib-only and takes its webhook from ``GOOGLE_CHAT_WEBHOOK_URL``.
"""

from __future__ import annotations

import argparse
import dataclasses
import logging
import os
import subprocess
import sys
from datetime import datetime, timezone
from typing import Iterable

try:
    from azure.storage.blob import BlobServiceClient
except ImportError:  # pragma: no cover - azure is absent in CI
    BlobServiceClient = None

try:
    from azure.core.exceptions import ResourceNotFoundError
except ImportError:  # pragma: no cover - same

    class ResourceNotFoundError(Exception):
        """Stand-in so the missing-container path is testable without azure."""


REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.append(REPO_ROOT)

from new_portal.rto_blob_snapshot import DEFAULT_CONTAINER_NAME

logger = logging.getLogger(__name__)

CONTAINER_NAME = DEFAULT_CONTAINER_NAME
CONNECTION_STRING_ENV_VAR = "AZURE_STORAGE_CONNECTION_STRING"
SEND_CHAT_ALERT = os.path.join(REPO_ROOT, "ops", "send_chat_alert.py")
DEFAULT_PIPELINE = "new_portal_rto_snapshot_watchdog"

# The sentence that would have shortened the November 2026 incident from two
# days to one. It belongs in every stale message this module produces.
OFF_VM_HINT = (
    "This check runs off the VM, so a failure here may mean the VM never "
    "started and no cron job fired at all."
)


@dataclasses.dataclass(frozen=True)
class WatchdogVerdict:
    is_stale: bool
    message: str


def _now() -> datetime:
    """Seam so tests can pin the reference instant."""
    return datetime.now(tz=timezone.utc)


def _as_utc(value: datetime) -> datetime:
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def newest_snapshot_age_days(
    blob_names_with_times: Iterable[tuple[str, datetime]], reference: datetime
) -> float | None:
    """Age in days of the most recent snapshot, or ``None`` if there are none."""
    newest: datetime | None = None
    for _name, modified in blob_names_with_times:
        modified = _as_utc(modified)
        if newest is None or modified > newest:
            newest = modified
    if newest is None:
        return None
    return (_as_utc(reference) - newest).total_seconds() / 86400


def list_snapshot_times(container_client) -> list[tuple[str, datetime]] | None:
    """Blob names and timestamps, or ``None`` when the container is unreachable.

    Absent and empty stay distinguishable: both alert, but only one of them
    means the watchdog itself is pointed at the wrong place.
    """
    try:
        return [(blob.name, blob.last_modified) for blob in container_client.list_blobs()]
    except ResourceNotFoundError:
        logger.error(
            "Container %s does not exist or cannot be read; treating as stale "
            "rather than as success.",
            CONTAINER_NAME,
        )
        return None


def build_container_client(
    *, connection_string: str | None = None, container_name: str = CONTAINER_NAME
):
    """A container client from the environment. Never reads config.yaml."""
    # Configuration before environment: a missing secret is the likelier
    # failure and the more actionable message, and it is checkable without
    # azure installed at all.
    connection_string = connection_string or os.environ.get(
        CONNECTION_STRING_ENV_VAR, ""
    ).strip()
    if not connection_string:
        raise RuntimeError(
            f"{CONNECTION_STRING_ENV_VAR} is not set; the watchdog cannot reach "
            f"blob storage. Set it as a repository secret."
        )
    if BlobServiceClient is None:
        raise RuntimeError(
            "azure-storage-blob is not installed; this watchdog needs it to "
            "read the snapshot container."
        )
    service = BlobServiceClient.from_connection_string(connection_string)
    return service.get_container_client(container_name)


def evaluate_snapshot_freshness(
    age_days: float | None, *, max_age_days: int
) -> WatchdogVerdict:
    """Whether the newest snapshot is recent enough to prove a run happened."""
    if age_days is None:
        return WatchdogVerdict(
            is_stale=True,
            message=(
                f"no snapshot found in container {CONTAINER_NAME}, so no V2 run "
                f"can be shown to have completed a fetch. {OFF_VM_HINT}"
            ),
        )
    if age_days > max_age_days:
        return WatchdogVerdict(
            is_stale=True,
            message=(
                f"newest snapshot in {CONTAINER_NAME} is {age_days:.1f} days "
                f"old, over the {max_age_days}-day limit. {OFF_VM_HINT}"
            ),
        )
    return WatchdogVerdict(
        is_stale=False,
        message=(
            f"newest snapshot in {CONTAINER_NAME} is {age_days:.1f} days old, "
            f"limit {max_age_days}"
        ),
    )


def send_failure_alert(*, pipeline: str, details: str) -> int:
    """Post one FAILED alert through the same script the ETL runtime uses."""
    command = [
        sys.executable,
        SEND_CHAT_ALERT,
        "--pipeline",
        pipeline,
        "--status",
        "FAILED",
        "--step",
        "snapshot_watchdog",
        "--hostname",
        "github-actions",
        "--details",
        details,
    ]
    completed = subprocess.run(command, check=False)
    return getattr(completed, "returncode", 0)


def main(argv: list[str] | None = None) -> int:
    """Exit 1 and alert when no recent snapshot proves a run happened."""
    parser = argparse.ArgumentParser(description="Off-VM snapshot watchdog.")
    parser.add_argument("--max-age-days", type=int, default=40)
    parser.add_argument("--container", default=CONTAINER_NAME)
    parser.add_argument("--pipeline", default=DEFAULT_PIPELINE)
    args = parser.parse_args(argv)

    container_client = build_container_client(container_name=args.container)
    listed = list_snapshot_times(container_client)
    age_days = (
        None if listed is None else newest_snapshot_age_days(listed, _now())
    )
    verdict = evaluate_snapshot_freshness(age_days, max_age_days=args.max_age_days)

    if not verdict.is_stale:
        logger.info("Snapshot watchdog passed: %s", verdict.message)
        return 0

    logger.error("Snapshot watchdog failed: %s", verdict.message)
    send_failure_alert(pipeline=args.pipeline, details=verdict.message)
    return 1


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    sys.exit(main())
