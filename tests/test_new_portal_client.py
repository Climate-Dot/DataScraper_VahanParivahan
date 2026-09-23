import unittest
from unittest import mock

from new_portal import client


def _fake_response(status_code=200, json_body=None):
    response = mock.Mock()
    response.status_code = status_code
    response.json.return_value = json_body if json_body is not None else {}
    return response


class NewPortalClientTests(unittest.TestCase):
    def test_start_session_hits_dashboard_page(self):
        portal = client.NewPortalClient(sleep_func=lambda _: None)
        with mock.patch.object(
            portal.session, "get", return_value=_fake_response()
        ) as mock_get:
            portal.start_session()

        mock_get.assert_called_once()
        called_url = mock_get.call_args.args[0]
        self.assertEqual(called_url, f"{client.BASE_URL}{client.DASHBOARD_PATH}")
        self.assertTrue(portal._session_started)

    def test_get_rtos_for_state_parses_json_and_auto_starts_session(self):
        portal = client.NewPortalClient(sleep_func=lambda _: None)
        rto_payload = [{"id": 1, "rtoCode": 12, "rtoName": "PUNE", "stateCode": "MH"}]

        with mock.patch.object(
            portal.session,
            "get",
            side_effect=[_fake_response(), _fake_response(json_body=rto_payload)],
        ) as mock_get:
            result = portal.get_rtos_for_state("MH")

        self.assertEqual(result, rto_payload)
        self.assertEqual(mock_get.call_count, 2)
        self.assertTrue(portal._session_started)

    def test_get_fuel_type_breakdown_includes_vehicle_classes_param(self):
        portal = client.NewPortalClient(sleep_func=lambda _: None)
        portal._session_started = True

        with mock.patch.object(
            portal.session, "get", return_value=_fake_response(json_body={"labels": []})
        ) as mock_get:
            portal.get_fuel_type_breakdown(
                state_code="MH",
                rto_code=12,
                from_year=2026,
                to_year=2026,
                vehicle_classes="Motor Car",
            )

        params = mock_get.call_args.kwargs["params"]
        self.assertEqual(params["vehicleClasses"], "Motor Car")
        self.assertEqual(params["stateCode"], "MH")

    def test_default_archive_types_match_all_statuses(self):
        portal = client.NewPortalClient(sleep_func=lambda _: None)
        portal._session_started = True

        with mock.patch.object(
            portal.session, "get", return_value=_fake_response(json_body=[])
        ) as mock_get:
            portal.get_duration_wise_registration(
                state_code="MH", rto_code=12, from_year=2026, to_year=2026
            )

        params = mock_get.call_args.kwargs["params"]
        self.assertEqual(params["archiveTypePA"], "PERMANENT_ARCHIVE")
        self.assertEqual(params["archiveTypeTA"], "TEMPORARY_ARCHIVE")
        self.assertEqual(params["archiveTypeNA"], "NA")

    def test_retries_then_succeeds(self):
        sleeps = []
        portal = client.NewPortalClient(
            retry_attempts=3, retry_backoff_seconds=1, sleep_func=sleeps.append
        )
        portal._session_started = True

        with mock.patch.object(
            portal.session,
            "get",
            side_effect=[_fake_response(status_code=500), _fake_response(json_body={"ok": True})],
        ):
            result = portal.get_status_distribution(
                state_code="MH", rto_code=12, from_year=2026, to_year=2026
            )

        self.assertEqual(result, {"ok": True})
        self.assertEqual(sleeps, [1])

    def test_get_dashboard_count_parses_indian_comma_formatted_total(self):
        portal = client.NewPortalClient(sleep_func=lambda _: None)
        portal._session_started = True

        with mock.patch.object(
            portal.session,
            "get",
            return_value=_fake_response(json_body={"totalTransactions": "2,18,788"}),
        ):
            result = portal.get_dashboard_count(
                state_code="MH", rto_code=12, from_year=2026, to_year=2026
            )

        self.assertEqual(result, 218788)

    def test_raises_new_portal_error_after_exhausting_retries(self):
        portal = client.NewPortalClient(
            retry_attempts=2, retry_backoff_seconds=1, sleep_func=lambda _: None
        )
        portal._session_started = True

        with mock.patch.object(
            portal.session, "get", return_value=_fake_response(status_code=503)
        ):
            with self.assertRaises(client.NewPortalError):
                portal.get_class_distribution(
                    state_code="MH", rto_code=12, from_year=2026, to_year=2026
                )


class ParseCommaFormattedIntTests(unittest.TestCase):
    def test_parses_indian_style_grouping(self):
        self.assertEqual(client.parse_comma_formatted_int("2,18,788"), 218788)

    def test_parses_western_style_grouping(self):
        self.assertEqual(client.parse_comma_formatted_int("30,337"), 30337)

    def test_parses_no_grouping(self):
        self.assertEqual(client.parse_comma_formatted_int("42"), 42)


if __name__ == "__main__":
    unittest.main()
