import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

from ops import send_chat_alert


class ChatAlertTests(unittest.TestCase):
    def test_resolve_webhook_url_prefers_environment_variable(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.yaml"
            config_path.write_text(
                "alerts:\n  google_chat_webhook_url: https://config.example.test\n",
                encoding="utf-8",
            )

            webhook_url = send_chat_alert.resolve_webhook_url(
                config_path=config_path,
                environ={"GOOGLE_CHAT_WEBHOOK_URL": "https://env.example.test"},
            )

        self.assertEqual(webhook_url, "https://env.example.test")

    def test_resolve_webhook_url_supports_nested_alerts_config(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.yaml"
            config_path.write_text(
                "alerts:\n  google_chat_webhook_url: https://chat.example.test\n",
                encoding="utf-8",
            )

            webhook_url = send_chat_alert.resolve_webhook_url(
                config_path=config_path,
                environ={},
            )

        self.assertEqual(webhook_url, "https://chat.example.test")

    def test_build_message_text_includes_operational_context(self):
        timestamp = datetime(2026, 7, 17, 4, 30, tzinfo=timezone.utc)

        message_text = send_chat_alert.build_message_text(
            pipeline="rto",
            status="failed",
            run_label="JUN 2026",
            step="preprocessing",
            exit_code=1,
            hostname="climate-dot-vm",
            headless="false",
            log_file="/var/log/rto.log",
            details="dbt model failure. Check the ETL log and dbt excerpt below.",
            details_file="/tmp/dbt_rto.log",
            log_excerpt="ERROR - No valid data fetched",
            details_excerpt="Database Error in model rto_wise_ev_data",
            timestamp=timestamp,
        )

        self.assertIn("[FAILED] RTO ETL pipeline", message_text)
        self.assertIn("Summary:", message_text)
        self.assertIn("- Run: JUN 2026", message_text)
        self.assertIn("- Failed step: preprocessing", message_text)
        self.assertIn("- Exit code: 1", message_text)
        self.assertIn("- Host: climate-dot-vm", message_text)
        self.assertIn("- Browser mode: headed (xvfb)", message_text)
        self.assertIn("Time (UTC): 2026-07-17 04:30:00", message_text)
        self.assertIn("- ETL log: /var/log/rto.log", message_text)
        self.assertIn("- Extra log: /tmp/dbt_rto.log", message_text)
        self.assertIn("Context:", message_text)
        self.assertIn("dbt model failure. Check the ETL log and dbt excerpt below.", message_text)
        self.assertIn("Recent ETL log lines:", message_text)
        self.assertIn("ERROR - No valid data fetched", message_text)
        self.assertIn("Recent extra log lines:", message_text)
        self.assertIn("Database Error in model rto_wise_ev_data", message_text)

    def test_build_message_text_success_uses_celebratory_defaults(self):
        timestamp = datetime(2026, 7, 17, 5, 45, tzinfo=timezone.utc)

        message_text = send_chat_alert.build_message_text(
            pipeline="oem",
            status="success",
            run_label="JUN 2026",
            step="completed",
            exit_code=0,
            hostname="climate-dot-vm",
            headless="false",
            log_file="/var/log/oem.log",
            timestamp=timestamp,
        )

        self.assertIn("🎉 [SUCCESS] OEM ETL pipeline", message_text)
        self.assertIn("- Run: JUN 2026", message_text)
        self.assertIn("- Final step: completed", message_text)
        self.assertIn("- Exit code: 0", message_text)
        self.assertIn("- Browser mode: headed (xvfb)", message_text)
        self.assertIn("- ETL log: /var/log/oem.log", message_text)
        self.assertIn("Everything finished cleanly. The data gremlins stayed off duty this run.", message_text)
        self.assertNotIn("Recent ETL log lines:", message_text)

    def test_build_recent_excerpt_prefers_interesting_lines(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = Path(tmpdir) / "etl.log"
            log_path.write_text(
                "\n".join(
                    [
                        "INFO - start",
                        "INFO - still running",
                        "ERROR - element lookup failed",
                        "Traceback (most recent call last):",
                        "ValueError: File is not a zip file",
                    ]
                ),
                encoding="utf-8",
            )

            excerpt = send_chat_alert.build_recent_excerpt(str(log_path))

        self.assertIn("ERROR - element lookup failed", excerpt)
        self.assertIn("Traceback (most recent call last):", excerpt)
        self.assertIn("ValueError: File is not a zip file", excerpt)
        self.assertNotIn("INFO - start", excerpt)

    @mock.patch("ops.send_chat_alert.request.urlopen")
    def test_send_google_chat_message_posts_expected_payload(self, mock_urlopen):
        mock_response = mock.Mock()
        mock_response.__enter__ = mock.Mock(return_value=mock_response)
        mock_response.__exit__ = mock.Mock(return_value=False)
        mock_response.status = 200
        mock_urlopen.return_value = mock_response

        send_chat_alert.send_google_chat_message(
            "https://chat.example.test",
            "Climate ETL alert test",
        )

        call_args = mock_urlopen.call_args
        self.assertIsNotNone(call_args)
        request_arg = call_args.args[0]
        self.assertEqual(request_arg.full_url, "https://chat.example.test")
        self.assertEqual(request_arg.get_method(), "POST")
        self.assertEqual(
            request_arg.get_header("Content-type"),
            "application/json; charset=UTF-8",
        )
        self.assertEqual(
            request_arg.data,
            b'{"text": "Climate ETL alert test"}',
        )
        self.assertEqual(call_args.kwargs["timeout"], 30)


if __name__ == "__main__":
    unittest.main()
