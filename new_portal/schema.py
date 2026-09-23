"""Column vocabulary for the V2 (new portal) RTO fact table.

Keeping this in one module means the CSV writer, the SQL migration and the
ingestor cannot drift apart — the ingestor does
``INSERT INTO final SELECT * FROM staging``, so column *order* is load-bearing,
not just column names.

See docs/new_portal_v2_design.md.
"""

from __future__ import annotations

import re

# The portal's own fuel dropdown (`<select id="vehicleFuel">` on
# /analytics/publicdashboard/vahan), scraped 2026-08-18: 34 values.
# These are the exact strings the API expects in `vehicleFuels[]` and the exact
# strings `fueltypedonutchart` returns in its `labels`.
PORTAL_FUEL_LABELS = [
    "BIO-CNG/BIO-GAS",
    "CNG ONLY",
    "DI-METHYL ETHER",
    "DIESEL",
    "DIESEL/HYBRID",
    "DUAL DIESEL/BIO CNG",
    "DUAL DIESEL/CNG",
    "DUAL DIESEL/LNG",
    "ELECTRIC(BOV)",
    "ETHANOL(E100)",
    "FLEX-FUEL(BIO-DIESEL)",
    "FLEX-FUEL(ETHANOL)",
    "FUEL CELL HYDROGEN",
    "HCNG",
    "HYDROGEN(ICE)",
    "LNG",
    "LPG ONLY",
    "METHANOL",
    "NOT APPLICABLE",
    "PETROL",
    "PETROL(E20)",
    "PETROL(E20)/CNG",
    "PETROL(E20)/HYBRID",
    "PETROL(E20)/HYBRID/CNG",
    "PETROL(E20)/LPG",
    "PETROL/CNG",
    "PETROL/HYBRID",
    "PETROL/HYBRID/CNG",
    "PETROL/LPG",
    "PETROL/METHANOL",
    "PLUG-IN HYBRID EV",
    "PURE EV",
    "SOLAR",
    "STRONG HYBRID EV",
]

# V1 fuel columns the new portal has no equivalent for. Per this repo's standing
# rule (CLAUDE.md safety rule 4), they are carried as always-NULL columns rather
# than dropped or zero-filled: a column that exists but is empty is honest about
# the source no longer reporting it; a 0 would be a fabricated count.
FUEL_COLUMNS_NOT_ON_NEW_PORTAL = ["bio_diesel_b100", "bio_methane"]


def fuel_label_to_column(label: str) -> str:
    """"PETROL(E20)/HYBRID/CNG" -> "petrol_e20_hybrid_cng".

    Reproduces V1's existing snake_case fuel column names exactly; verified
    2026-08-18 that all 34 portal labels round-trip onto real V1 columns.
    "BIO-CNG/BIO-GAS" is the one label whose punctuation would otherwise
    collapse to `bio_cng_bio_gas` differently, so it is normalized first.
    """
    normalized = label.replace("BIO-CNG/BIO-GAS", "BIO CNG BIO GAS")
    slug = re.sub(r"[^A-Za-z0-9]+", "_", normalized).strip("_").lower()
    return re.sub(r"_+", "_", slug)


FUEL_LABEL_TO_COLUMN = {label: fuel_label_to_column(label) for label in PORTAL_FUEL_LABELS}

# Sorted so the physical column order is stable and reviewable, and so the two
# portal-absent columns sit in the same alphabetical run as the rest rather than
# being tacked on the end.
FUEL_COLUMNS = sorted(set(FUEL_LABEL_TO_COLUMN.values()) | set(FUEL_COLUMNS_NOT_ON_NEW_PORTAL))

# Archive-status scopes. Both are defined, but only ALL_STATUSES is ingested
# (decision 6, revised 2026-08-19).
#
# ALL_STATUSES answers "how many vehicles were registered that month" — a
# historical event count that never changes. ACTIVE answers "how many of those
# are still valid right now", which decays as vehicles are scrapped, archived or
# deregistered: measured 2026-08-19 on Pune, ~97% of 2024 registrations were
# still active, ~50% of early-2019, ~39% of 2015. Storing that in a dated fact
# row records a value whose meaning depends on the ingestion date.
#
# Since V2 only ingests forward, every month is captured while fresh — and when
# fresh the two scopes agree to within ~0.1%. So ACTIVE would have doubled the
# API calls to store a near-identical number that then goes stale in place.
#
# status_scope remains a real column and part of the grain, so adding ACTIVE
# back later is a fetch flag plus a re-run, not a schema migration.
STATUS_SCOPE_ACTIVE = "ACTIVE"
STATUS_SCOPE_ALL = "ALL_STATUSES"
STATUS_SCOPES = [STATUS_SCOPE_ACTIVE, STATUS_SCOPE_ALL]

# What the pipeline actually pulls by default.
DEFAULT_STATUS_SCOPES = [STATUS_SCOPE_ALL]

# Grain key (decision 3). No `day`: the source has no day-level granularity, and
# V1's `day` column was only ever a monthly report's nominal date.
KEY_COLUMNS = ["date", "state_code", "rto_code", "vehicle_class", "status_scope"]

DIMENSION_COLUMNS = [
    "year",
    "month",
    "date",
    "state",
    "state_code",
    "rto_code",
    "rto_name",
    "legacy_rto_code",
    "vehicle_class",
    "vehicle_type",
    "vehicle_category",
    "vehicle_use_type",
    "status_scope",
]

# Physical column order of fact_ev_data_by_rto_v2, minus `inserted_at` (which
# the ingestor appends itself). The CSV header must match this exactly.
CSV_COLUMNS = DIMENSION_COLUMNS + FUEL_COLUMNS + ["total"]

MONTH_NAME_TO_NUMBER = {
    "January": 1,
    "February": 2,
    "March": 3,
    "April": 4,
    "May": 5,
    "June": 6,
    "July": 7,
    "August": 8,
    "September": 9,
    "October": 10,
    "November": 11,
    "December": 12,
}


def parse_year_as_string(year_as_string: str) -> tuple[int, int]:
    """"2026-August" -> (2026, 8).

    `durationWiseRegistrationTable`'s numeric `year` field is always 0; the real
    year and month live only in this string (confirmed 2026-08-12).
    """
    year_part, _, month_part = year_as_string.partition("-")
    month_part = month_part.strip()
    if month_part not in MONTH_NAME_TO_NUMBER:
        raise ValueError(f"unrecognized yearAsString month: {year_as_string!r}")
    return int(year_part), MONTH_NAME_TO_NUMBER[month_part]
