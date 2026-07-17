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
            details="Detailed dbt output: /tmp/dbt_rto.log",
            timestamp=timestamp,
        )

        self.assertIn("[FAILED] RTO ETL failed", message_text)
        self.assertIn("Run: JUN 2026", message_text)
        self.assertIn("Step: preprocessing", message_text)
        self.assertIn("Exit code: 1", message_text)
        self.assertIn("Host: climate-dot-vm", message_text)
        self.assertIn("Headless: false", message_text)
        self.assertIn("Log file: /var/log/rto.log", message_text)
        self.assertIn("Time (UTC): 2026-07-17 04:30:00", message_text)
        self.assertIn("Detailed dbt output: /tmp/dbt_rto.log", message_text)

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
