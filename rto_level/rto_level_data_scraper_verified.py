"""RTO scraper that checks what it selected and what it downloaded.

A new module rather than a change to ``rto_level_data_scraper.py``, which cron
calls. Switch cron over only after a full run has been compared against the
current scraper.

Why this exists: an audit of all 270,403 archived reports on 2026-11-05 found 45
files sitting in the wrong office's folder across August, September and October
2026, plus 15 files holding the wrong month or year between 2015 and 2024. In
every one of the 45 the file belonged to the **7th office** in that state's
dropdown -- 19 states, 19 donors, no exceptions. The office panel renders 200px
tall over 4,480px of content, so exactly 8 rows are visible, and DOM item 8 (a
header plus seven offices) is the last row that fits unscrolled. The pipeline's
``find_element`` waits on ``element_to_be_clickable``, which returns rows clipped
outside that panel, and no window size was set, so the geometry -- and therefore
the donor -- was identical on every run.

The original flow clicks each control blindly and sleeps a fixed interval, so a
selection that did not land is never noticed, and the download path is built from
the *intended* labels rather than the file. Three additions close that:

1. every dropdown selection is read back off its own label and asserted
2. the office row is scrolled into the panel before being clicked
3. the downloaded workbook must state the requested office, month and year in
   cell ``A1`` before it is accepted

Any of these failing raises, so the existing retry loop re-runs the office
instead of storing a wrong report. An empty report is accepted: Vahan serves
those with a header row and no identity, and an office with no registrations in
a month is not an error.
"""

from __future__ import annotations

import logging
import os
import sys
import time

from openpyxl import load_workbook
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

repo_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if repo_path not in sys.path:
    sys.path.append(repo_path)

from pipeline_logging import configure_pipeline_logging
from rto_level.report_identity import ReportIdentityError, assert_report_identity
from rto_level.rto_level_data_scraper import RTODataScraper
from utils import (
    VAHAN_DASHBOARD_URL,
    configure_chrome_options,
    create_directory_if_not_exists,
    find_element,
    open_page,
    wait_for_expected_download,
)

configure_pipeline_logging()

# The dropdown panel's geometry decided which office leaked. Pin the window so
# it is a deliberate value rather than whatever xvfb happened to hand out.
DEFAULT_WINDOW_SIZE = "1400,1800"

REFRESH_BUTTON_CSS = (
    "button[class='ui-button ui-widget ui-state-default ui-corner-all "
    "ui-button-text-icon-left button']"
)


class SelectionNotApplied(Exception):
    """A control still shows something other than what was selected."""


class VerifiedRTODataScraper(RTODataScraper):
    """``RTODataScraper`` that refuses a report it cannot prove is the right one."""

    def __init__(self, *args, window_size: str | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.window_size = window_size or os.environ.get(
            "VAHAN_WINDOW_SIZE", DEFAULT_WINDOW_SIZE
        )

    @staticmethod
    def read_label(browser, label_id):
        """Return the visible text of a PrimeFaces control's label."""
        return browser.execute_script(
            "const node = document.getElementById(arguments[0]);"
            "return node ? (node.textContent || '').trim() : null;",
            label_id,
        )

    def assert_selection(self, browser, label_id, expected_fragment, control):
        """Raise unless ``control`` now shows ``expected_fragment``."""
        shown = self.read_label(browser, label_id)
        if shown is None:
            raise SelectionNotApplied(
                f"{control}: label {label_id!r} is not present on the page"
            )
        if expected_fragment not in shown:
            raise SelectionNotApplied(
                f"{control}: expected {expected_fragment!r}, shows {shown!r}"
            )
        logging.debug("Verified %s selection: %s", control, shown)

    def select_office(self, browser, rto_office_code, context):
        """Click an office row, scrolling it into the panel first."""
        option = find_element(
            browser,
            "xpath",
            self.build_rto_option_xpath(rto_office_code),
            step="select_rto_office",
            context=context,
        )
        # element_to_be_clickable does not mean "inside the scrollable panel".
        browser.execute_script(
            "arguments[0].scrollIntoView({block: 'center'});", option
        )
        time.sleep(0.4)
        option.click()
        time.sleep(2)
        self.assert_selection(
            browser, "selectedRto_label", f"- {rto_office_code}(", "office"
        )

    @staticmethod
    def read_report_identity(report_path):
        """Return cell ``A1`` and whether the report holds any data rows."""
        workbook = load_workbook(report_path, data_only=True)
        sheet = workbook.active
        title = sheet["A1"].value
        data_rows = sum(
            1
            for row in sheet.iter_rows(min_row=5, max_row=sheet.max_row)
            if row[1].value is not None and str(row[1].value).strip()
        )
        return (str(title) if title is not None else None), data_rows

    def verify_downloaded_report(
        self, report_path, *, rto_office_code, month_label, year_label
    ):
        """Raise unless the workbook states the office and period requested."""
        title, data_rows = self.read_report_identity(report_path)
        assert_report_identity(
            title,
            expected_code=rto_office_code,
            expected_month=month_label,
            expected_year=str(year_label),
            allow_missing_identity=data_rows == 0,
        )
        return data_rows

    def extract_rto_level_data(
        self, state_label, rto_label, year_label, month_label
    ):
        """Download one office-month, proving the file is what was asked for."""
        state_folder_name = self.sanitize_state_folder_name(state_label)
        rto_folder_name = self.build_rto_folder_name(rto_label)
        if not rto_folder_name:
            raise ValueError(f"Unable to parse RTO label: {rto_label}")
        rto_office_code = rto_folder_name.rsplit("_", 1)[1]

        download_path = self.build_download_directory(
            state_label, rto_label, year_label, month_label
        )
        create_directory_if_not_exists(download_path)

        browser_options = webdriver.ChromeOptions()
        browser_options.add_argument(f"--window-size={self.window_size}")
        configure_chrome_options(browser_options, download_path)
        browser = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=browser_options,
        )

        expected_report_path = os.path.join(download_path, "reportTable.xlsx")
        if os.path.exists(expected_report_path):
            os.remove(expected_report_path)

        context = {
            "pipeline": "rto-verified",
            "state": state_label,
            "rto_label": rto_label,
            "rto_code": rto_office_code,
            "year": year_label,
            "month": month_label,
        }

        try:
            retries = 0
            last_exception = None
            while retries < self.max_retries:
                try:
                    open_page(
                        browser,
                        VAHAN_DASHBOARD_URL,
                        step="initial_page_load",
                        context=context,
                    )

                    state_control = find_element(
                        browser,
                        "xpath",
                        '//label[starts-with(text(), "All Vahan4 Running States")]',
                        step="open_state_dropdown",
                        context=context,
                    )
                    # The state control carries a generated JSF id; take it from
                    # the element rather than hard-coding one that can change.
                    state_label_id = state_control.get_attribute("id")
                    state_control.click()
                    time.sleep(2)
                    find_element(
                        browser,
                        "xpath",
                        f'//li[starts-with(text(), "{state_label}")]',
                        step="select_state",
                        context=context,
                    ).click()
                    time.sleep(2)
                    self.assert_selection(
                        browser, state_label_id, state_label, "state"
                    )

                    find_element(
                        browser,
                        "xpath",
                        '//label[starts-with(text(), "All Vahan4 Running Office")]',
                        step="open_rto_dropdown",
                        context=context,
                    ).click()
                    time.sleep(1)
                    self.select_office(browser, rto_office_code, context)

                    find_element(
                        browser,
                        "id",
                        "yaxisVar_label",
                        step="open_y_axis_dropdown",
                        context=context,
                    ).click()
                    time.sleep(2)
                    find_element(
                        browser,
                        "id",
                        "yaxisVar_1",
                        step="select_y_axis_vehicle_class",
                        context=context,
                    ).click()
                    time.sleep(2)

                    find_element(
                        browser,
                        "id",
                        "xaxisVar_label",
                        step="open_x_axis_dropdown",
                        context=context,
                    ).click()
                    time.sleep(1)
                    find_element(
                        browser,
                        "xpath",
                        "//ul[@id='xaxisVar_items']/li[text()='Fuel']",
                        step="select_x_axis_fuel",
                        context=context,
                    ).click()
                    time.sleep(2)

                    find_element(
                        browser,
                        "id",
                        "selectedYear_label",
                        step="open_year_dropdown",
                        context=context,
                    ).click()
                    time.sleep(2)
                    find_element(
                        browser,
                        "xpath",
                        f"//ul[@id='selectedYear_items']/li[text()='{year_label}']",
                        step="select_year",
                        context=context,
                    ).click()
                    time.sleep(5)
                    # The year dropdown defaults to the current year, which is
                    # how historical folders ended up holding current-year data.
                    self.assert_selection(
                        browser, "selectedYear_label", str(year_label), "year"
                    )

                    find_element(
                        browser,
                        "css",
                        REFRESH_BUTTON_CSS,
                        step="click_main_refresh",
                        context=context,
                    ).click()
                    time.sleep(5)

                    find_element(
                        browser,
                        "id",
                        "groupingTable:selectMonth_label",
                        step="open_month_dropdown",
                        context=context,
                    ).click()
                    time.sleep(2)
                    find_element(
                        browser,
                        "xpath",
                        "//ul[@id='groupingTable:selectMonth_items']"
                        f"/li[text()='{month_label}']",
                        step="select_month",
                        context=context,
                    ).click()
                    time.sleep(2)
                    self.assert_selection(
                        browser,
                        "groupingTable:selectMonth_label",
                        month_label,
                        "month",
                    )
                    # Refresh can reset the year, so check it survived.
                    self.assert_selection(
                        browser,
                        "selectedYear_label",
                        str(year_label),
                        "year after refresh",
                    )

                    find_element(
                        browser,
                        "id",
                        "groupingTable:xls",
                        step="download_report",
                        context=context,
                    ).click()
                    wait_for_expected_download(download_path)

                    data_rows = self.verify_downloaded_report(
                        expected_report_path,
                        rto_office_code=rto_office_code,
                        month_label=month_label,
                        year_label=year_label,
                    )
                    logging.info(
                        "Downloaded and verified RTO report state=%s rto=%s "
                        "year=%s month=%s data_rows=%s",
                        state_folder_name,
                        rto_folder_name,
                        year_label,
                        month_label,
                        data_rows,
                    )
                    return
                except (SelectionNotApplied, ReportIdentityError) as error:
                    # The run picked up the wrong office or period. Drop the file
                    # so a failed office never leaves a plausible-looking report.
                    last_exception = error
                    retries += 1
                    if os.path.exists(expected_report_path):
                        try:
                            os.remove(expected_report_path)
                        except OSError:
                            pass
                    logging.warning(
                        "Rejected RTO report attempt=%s/%s state=%s rto=%s "
                        "year=%s month=%s reason=%s",
                        retries,
                        self.max_retries,
                        state_label,
                        rto_office_code,
                        year_label,
                        month_label,
                        error,
                    )
                    time.sleep(self.retry_delay)

            raise last_exception
        finally:
            browser.quit()

    @staticmethod
    def sanitize_state_folder_name(state_label):
        """Match the production folder naming for state directories."""
        import re

        return re.sub(r"[^a-zA-Z\s]", " ", state_label).rstrip()


def main():
    """Run the verified scraper over the same arguments the production one takes."""
    from rto_level import rto_level_data_scraper as production

    production.RTODataScraper = VerifiedRTODataScraper
    production.main()


if __name__ == "__main__":
    main()
