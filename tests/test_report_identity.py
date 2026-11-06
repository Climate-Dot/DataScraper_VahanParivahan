import os
import sys
import unittest

repo_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if repo_path not in sys.path:
    sys.path.append(repo_path)

from rto_level.report_identity import (
    ReportIdentityError,
    assert_report_identity,
    describe_unparseable_title,
    parse_report_title,
)

OFFICE_TITLE = (
    "Vehicle Class Wise Fuel Data  of BARRACKPORE ARTO - WB23 , "
    "West Bengal (OCT,2026)"
)


class ParseReportTitleTests(unittest.TestCase):
    def test_parses_office_name_code_state_and_period(self):
        identity = parse_report_title(OFFICE_TITLE)
        self.assertEqual(identity.rto_name, "BARRACKPORE ARTO")
        self.assertEqual(identity.rto_code, "WB23")
        self.assertEqual(identity.state, "West Bengal")
        self.assertEqual(identity.month, "OCT")
        self.assertEqual(identity.year, "2026")

    def test_parses_office_name_containing_commas_and_hyphens(self):
        title = (
            "Vehicle Class Wise Fuel Data  of Dy.REGIONAL TRANSPORT "
            "OFFICE,MIRA BHAYANDAR - MH58 , Maharashtra (MAY,2026)"
        )
        identity = parse_report_title(title)
        self.assertEqual(identity.rto_code, "MH58")
        self.assertEqual(identity.state, "Maharashtra")

    def test_returns_none_for_state_wide_report(self):
        title = "Vehicle Class Wise Fuel Data  of Kerala (OCT,2026)"
        self.assertIsNone(parse_report_title(title))

    def test_returns_none_when_month_is_missing(self):
        title = "Vehicle Class Wise Fuel Data  of PATAUDI - HR76 , Haryana (2018)"
        self.assertIsNone(parse_report_title(title))

    def test_returns_none_for_empty_title(self):
        self.assertIsNone(parse_report_title(None))
        self.assertIsNone(parse_report_title(""))


class DescribeUnparseableTitleTests(unittest.TestCase):
    def test_names_the_state_wide_case(self):
        message = describe_unparseable_title(
            "Vehicle Class Wise Fuel Data  of Kerala (OCT,2026)"
        )
        self.assertIn("whole state", message)

    def test_names_the_missing_month_case(self):
        message = describe_unparseable_title(
            "Vehicle Class Wise Fuel Data  of PATAUDI - HR76 , Haryana (2018)"
        )
        self.assertIn("no month", message)

    def test_names_the_missing_title_case(self):
        self.assertIn("no title", describe_unparseable_title(None))


class AssertReportIdentityTests(unittest.TestCase):
    def test_accepts_the_requested_office_and_period(self):
        identity = assert_report_identity(
            OFFICE_TITLE,
            expected_code="WB23",
            expected_month="OCT",
            expected_year="2026",
        )
        self.assertEqual(identity.rto_code, "WB23")

    def test_code_comparison_ignores_case(self):
        assert_report_identity(
            OFFICE_TITLE,
            expected_code="wb23",
            expected_month="OCT",
            expected_year="2026",
        )

    def test_rejects_a_different_office(self):
        with self.assertRaises(ReportIdentityError) as caught:
            assert_report_identity(
                OFFICE_TITLE,
                expected_code="WB999",
                expected_month="OCT",
                expected_year="2026",
            )
        self.assertIn("WB23", str(caught.exception))

    def test_rejects_a_different_month(self):
        with self.assertRaises(ReportIdentityError) as caught:
            assert_report_identity(
                OFFICE_TITLE,
                expected_code="WB23",
                expected_month="SEP",
                expected_year="2026",
            )
        self.assertIn("2026/OCT", str(caught.exception))

    def test_rejects_a_different_year(self):
        with self.assertRaises(ReportIdentityError):
            assert_report_identity(
                OFFICE_TITLE,
                expected_code="WB23",
                expected_month="OCT",
                expected_year="2025",
            )

    def test_rejects_a_state_wide_report_in_an_office_slot(self):
        with self.assertRaises(ReportIdentityError) as caught:
            assert_report_identity(
                "Vehicle Class Wise Fuel Data  of Kerala (OCT,2026)",
                expected_code="KL13",
                expected_month="OCT",
                expected_year="2026",
            )
        self.assertIn("whole state", str(caught.exception))

    def test_allows_a_missing_identity_when_the_report_is_empty(self):
        self.assertIsNone(
            assert_report_identity(
                "Vehicle Class Wise Fuel Data ",
                expected_code="WB999",
                expected_month="OCT",
                expected_year="2026",
                allow_missing_identity=True,
            )
        )


if __name__ == "__main__":
    unittest.main()
