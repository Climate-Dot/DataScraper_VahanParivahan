"""Read a Vahan report's own identity out of its first cell and check it.

Every ``reportTable.xlsx`` Vahan serves states in cell ``A1`` which office and
which period it covers, for example::

    Vehicle Class Wise Fuel Data  of BARRACKPORE ARTO - WB23 , West Bengal (OCT,2026)

The scraping pipeline never read that cell, so a download that came back for the
wrong office or the wrong period was saved under the requested name and ingested
as if it were correct. These helpers make the file state its own identity so a
caller can refuse it.

Kept free of Selenium and database imports so it stays importable under the
CI-only requirements and can be unit tested without a browser.
"""

from __future__ import annotations

import re
from typing import NamedTuple

# "... of <NAME> - <CODE> , <STATE> (<MON>,<YEAR>)". The office name may itself
# contain hyphens and commas, so the code is anchored on " - " before the comma
# that precedes the state.
_TITLE_PATTERN = re.compile(
    r"\sof\s+(?P<name>.+?)\s+-\s+(?P<code>[A-Za-z0-9]+)\s*,\s*"
    r"(?P<state>.+?)\s*\((?P<month>[A-Z]{3}),(?P<year>\d{4})\)\s*$"
)

# A state-wide report carries no office at all: "... of Kerala (OCT,2026)".
_STATE_ONLY_PATTERN = re.compile(
    r"\sof\s+(?P<state>[^-,]+?)\s*\((?P<month>[A-Z]{3}),(?P<year>\d{4})\)\s*$"
)

# A report whose month was never applied carries only the year: "... (2018)".
_YEAR_ONLY_PATTERN = re.compile(r"\((?P<year>\d{4})\)\s*$")


class ReportIdentity(NamedTuple):
    """What a report says it is."""

    rto_name: str
    rto_code: str
    state: str
    month: str
    year: str


class ReportIdentityError(Exception):
    """A report's title is missing, unreadable, or names something else."""


def parse_report_title(title: str | None) -> ReportIdentity | None:
    """Return the identity a title states, or ``None`` if it states no office."""
    if not title:
        return None

    match = _TITLE_PATTERN.search(title)
    if match is None:
        return None

    return ReportIdentity(
        rto_name=match.group("name").strip(),
        rto_code=match.group("code").strip(),
        state=match.group("state").strip(),
        month=match.group("month"),
        year=match.group("year"),
    )


def describe_unparseable_title(title: str | None) -> str:
    """Explain why a title names no office, for a log line or an error."""
    if not title:
        return "report has no title cell"
    if _STATE_ONLY_PATTERN.search(title):
        return "title names a whole state, not an office"
    if _YEAR_ONLY_PATTERN.search(title) and "," not in title[-10:]:
        return "title carries a year but no month"
    return "title does not match the expected report format"


def assert_report_identity(
    title: str | None,
    *,
    expected_code: str,
    expected_month: str,
    expected_year: str,
    allow_missing_identity: bool = False,
) -> ReportIdentity | None:
    """Raise unless the title names exactly the office and period requested.

    ``allow_missing_identity`` lets an empty report through: Vahan serves those
    with a header row and no identity, and an office legitimately having no
    registrations in a month is not an error.
    """
    identity = parse_report_title(title)
    if identity is None:
        if allow_missing_identity:
            return None
        raise ReportIdentityError(
            f"{describe_unparseable_title(title)}; expected "
            f"{expected_code} {expected_month} {expected_year}: {title!r}"
        )

    if identity.rto_code.upper() != expected_code.upper():
        raise ReportIdentityError(
            f"report is for office {identity.rto_code}, not {expected_code} "
            f"({identity.rto_name!r})"
        )

    if identity.month != expected_month or identity.year != expected_year:
        raise ReportIdentityError(
            f"report is for {identity.year}/{identity.month}, not "
            f"{expected_year}/{expected_month}"
        )

    return identity
