from __future__ import annotations

import logging
import time


def connect_with_retry(
    connect_callable,
    *,
    attempts=5,
    base_wait_seconds=20,
    sleep_func=time.sleep,
    logger=None,
    connection_error_types=(Exception,),
):
    active_logger = logger or logging.getLogger(__name__)
    last_error = None

    for attempt in range(attempts):
        try:
            connection = connect_callable()
            active_logger.info(
                "Successfully connected to the database on attempt %s.",
                attempt + 1,
            )
            return connection
        except connection_error_types as exc:
            last_error = exc
            wait_time = base_wait_seconds * attempt
            active_logger.warning("Database connection attempt %s failed: %s", attempt + 1, exc)
            if attempt < attempts - 1:
                active_logger.info("Retrying in %s seconds...", wait_time)
                sleep_func(wait_time)
        except Exception:
            raise

    if last_error is not None:
        raise last_error

    raise RuntimeError("Database connection retry loop exited unexpectedly.")
