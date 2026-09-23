from __future__ import annotations

import logging
import time

import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://analytics.parivahan.gov.in"
DASHBOARD_PATH = "/analytics/publicdashboard/vahan"
JSON_RTOS_PATH = "/analytics/json_rtos"
DURATION_WISE_REGISTRATION_PATH = (
    "/analytics/publicdashboard/vahandashboard/durationWiseRegistrationTable"
)
FUEL_TYPE_DONUT_PATH = "/analytics/publicdashboard/vahandashboard/fueltypedonutchart"
CLASS_DISTRIBUTION_PATH = "/analytics/publicdashboard/vahandashboard/classdistribution"
CATEGORIES_DONUT_PATH = "/analytics/publicdashboard/vahandashboard/categoriesdonutchart"
STATUS_DISTRIBUTION_PATH = "/analytics/publicdashboard/vahandashboard/statusdistribution"
DASHBOARD_COUNT_PATH = "/analytics/publicdashboard/vahan/registration/dashboardcount"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# archiveType* params, confirmed via the dashboard's own checkbox values
# (see docs/new_portal_v2_design.md, decision 6). ACTIVE_ONLY matches the
# portal's own default view; ALL_STATUSES matches today's historical Vahan
# `total` semantics and is what the Phase 0 reconciliation check validated.
ARCHIVE_TYPES_ACTIVE_ONLY = {
    "archiveTypeAC": "ACTIVE_COMPLIANT",
    "archiveTypeANC": "ACTIVE_NON_COMPLIANT",
    "archiveTypePA": "",
    "archiveTypeTA": "",
    "archiveTypeNA": "",
}

ARCHIVE_TYPES_ALL_STATUSES = {
    "archiveTypeAC": "ACTIVE_COMPLIANT",
    "archiveTypeANC": "ACTIVE_NON_COMPLIANT",
    "archiveTypePA": "PERMANENT_ARCHIVE",
    "archiveTypeTA": "TEMPORARY_ARCHIVE",
    "archiveTypeNA": "NA",
}

DEFAULT_TIMEOUT_SECONDS = 20
DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_RETRY_BACKOFF_SECONDS = 5


class NewPortalError(Exception):
    """Raised when the new portal returns a non-2xx response after retries."""


class NewPortalClient:
    """Thin session-based client for analytics.parivahan.gov.in.

    One client instance holds one warm session (cookie jar) that should be
    reused across an entire ingestion run — the portal's rate limit is tied
    to session creation, not to API call volume (confirmed 2026-08-12: a
    26-call burst against one reused session saw zero throttling).
    """

    def __init__(
        self,
        *,
        timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
        retry_attempts: int = DEFAULT_RETRY_ATTEMPTS,
        retry_backoff_seconds: int = DEFAULT_RETRY_BACKOFF_SECONDS,
        sleep_func=time.sleep,
    ) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self.timeout_seconds = timeout_seconds
        self.retry_attempts = retry_attempts
        self.retry_backoff_seconds = retry_backoff_seconds
        self.sleep_func = sleep_func
        self._session_started = False

    def start_session(self) -> None:
        """Load the dashboard page once to pick up the session cookies."""
        self._get_raw(DASHBOARD_PATH, params={"lang": "en"})
        self._session_started = True

    def _get_raw(self, path: str, params: dict) -> requests.Response:
        url = f"{BASE_URL}{path}"
        last_error: Exception | None = None
        for attempt in range(1, self.retry_attempts + 1):
            try:
                response = self.session.get(
                    url, params=params, timeout=self.timeout_seconds
                )
                if response.status_code == 200:
                    return response
                last_error = NewPortalError(
                    f"{url} returned HTTP {response.status_code} "
                    f"(attempt {attempt}/{self.retry_attempts})"
                )
            except requests.RequestException as exc:
                last_error = exc

            logger.warning(
                "new_portal request failed (%s), attempt %s/%s: %s",
                path,
                attempt,
                self.retry_attempts,
                last_error,
            )
            if attempt < self.retry_attempts:
                self.sleep_func(self.retry_backoff_seconds * attempt)

        raise NewPortalError(f"{url} failed after {self.retry_attempts} attempts") from last_error

    def _get_json(self, path: str, params: dict):
        if not self._session_started:
            self.start_session()
        return self._get_raw(path, params).json()

    def get_rtos_for_state(self, state_code: str) -> list[dict]:
        """RTO dimension lookup: [{id, rtoCode, rtoName, stateCode}, ...]."""
        return self._get_json(JSON_RTOS_PATH, {"stateCode": state_code})

    def get_duration_wise_registration(
        self,
        *,
        state_code: str,
        rto_code,
        from_year: int,
        to_year: int,
        vehicle_classes: str | None = None,
        vehicle_fuels: list[str] | None = None,
        vehicle_category_groups: list[str] | None = None,
        archive_types: dict = ARCHIVE_TYPES_ALL_STATUSES,
    ) -> list[dict]:
        """One row per calendar month: [{yearAsString, registeredVehicleCount, ...}, ...].

        This is the only endpoint that breaks results down by month, and it
        honours `vehicleClasses` and `vehicleFuels[]` together (verified
        2026-08-18) — which is what makes the monthly class x fuel cross-tab
        reachable at all. Summing across fuels for one class reproduces that
        class's own monthly totals exactly.

        Like get_fuel_type_breakdown, `vehicle_classes` must be an exact label
        from get_class_distribution; the sibling `vehicleType` param no-ops.
        """
        params = {
            "stateCode": state_code,
            "rtoCode": rto_code,
            "fromYear": from_year,
            "toYear": to_year,
            "calendarType": 3,
            "timePeriod": 0,
            "fitnessCheck": 0,
            "vehicleType": "",
            "vehicleClasses": vehicle_classes or "",
            **archive_types,
        }
        if vehicle_fuels:
            params["vehicleFuels[]"] = vehicle_fuels
        if vehicle_category_groups:
            params["vehicleCategoryGroup[]"] = vehicle_category_groups
        return self._get_json(DURATION_WISE_REGISTRATION_PATH, params)

    def get_fuel_type_breakdown(
        self,
        *,
        state_code: str,
        rto_code,
        from_year: int,
        to_year: int,
        vehicle_classes: str | None = None,
        vehicle_category_groups: list[str] | None = None,
        archive_types: dict = ARCHIVE_TYPES_ALL_STATUSES,
    ) -> dict:
        """Full per-fuel breakdown in one call: {"labels": [...], "data": [...]}.

        vehicle_classes must be an exact label match from get_class_distribution
        (e.g. "Motor Car") — the sibling param `vehicleType` silently no-ops
        and does NOT scope this endpoint (confirmed 2026-08-12).
        """
        params = {
            "stateCode": state_code,
            "rtoCode": rto_code,
            "fromYear": from_year,
            "toYear": to_year,
            "calendarType": 3,
            "timePeriod": 0,
            "fitnessCheck": 0,
            "vehicleType": "",
            "vehicleClasses": vehicle_classes or "",
            **archive_types,
        }
        if vehicle_category_groups:
            params["vehicleCategoryGroup[]"] = vehicle_category_groups
        return self._get_json(FUEL_TYPE_DONUT_PATH, params)

    def get_class_distribution(
        self,
        *,
        state_code: str,
        rto_code,
        from_year: int,
        to_year: int,
        archive_types: dict = ARCHIVE_TYPES_ALL_STATUSES,
    ) -> dict:
        """Full per-vehicle_class breakdown in one call: {"labels": [...], "data": [...]}."""
        params = {
            "stateCode": state_code,
            "rtoCode": rto_code,
            "fromYear": from_year,
            "toYear": to_year,
            "calendarType": 3,
            "timePeriod": 0,
            "fitnessCheck": 0,
            "vehicleType": "",
            **archive_types,
        }
        return self._get_json(CLASS_DISTRIBUTION_PATH, params)

    def get_status_distribution(
        self,
        *,
        state_code: str,
        rto_code,
        from_year: int,
        to_year: int,
    ) -> dict:
        """Full per-archive-status breakdown in one call."""
        params = {
            "stateCode": state_code,
            "rtoCode": rto_code,
            "fromYear": from_year,
            "toYear": to_year,
            "calendarType": 3,
            "timePeriod": 0,
            "fitnessCheck": 0,
            "vehicleType": "",
            **ARCHIVE_TYPES_ALL_STATUSES,
        }
        return self._get_json(STATUS_DISTRIBUTION_PATH, params)

    def get_dashboard_count(
        self,
        *,
        state_code: str,
        rto_code,
        from_year: int,
        to_year: int,
        vehicle_fuels: list[str] | None = None,
        vehicle_category_groups: list[str] | None = None,
        archive_types: dict = ARCHIVE_TYPES_ALL_STATUSES,
    ) -> int:
        """The dashboard's own headline total-count tile, parsed to an int.

        Year-level only (no month breakdown) — not a substitute for
        get_duration_wise_registration's per-month rows, but a genuinely
        independent server-computed total worth cross-checking ingested
        totals against (see docs/new_portal_v2_design.md, decision 9).
        """
        params = {
            "stateCode": state_code,
            "rtoCode": rto_code,
            "fromYear": from_year,
            "toYear": to_year,
            "timePeriod": 0,
            "fitnessCheck": 0,
            "vehicleType": "",
            **archive_types,
        }
        if vehicle_fuels:
            params["vehicleFuels[]"] = vehicle_fuels
        if vehicle_category_groups:
            params["vehicleCategoryGroup[]"] = vehicle_category_groups
        payload = self._get_json(DASHBOARD_COUNT_PATH, params)
        return parse_comma_formatted_int(payload["totalTransactions"])


def parse_comma_formatted_int(value: str) -> int:
    """Parses an Indian-style comma-grouped count string, e.g. "2,18,788"."""
    return int(value.replace(",", ""))
