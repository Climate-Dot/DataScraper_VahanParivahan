#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import socket
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib import error, request

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = REPO_ROOT / "config.yaml"
WEBHOOK_ENV_VAR = "GOOGLE_CHAT_WEBHOOK_URL"


def normalize_scalar(value: str) -> str:
    cleaned_value = value.strip()
    if len(cleaned_value) >= 2 and cleaned_value[0] == cleaned_value[-1]:
        if cleaned_value[0] in {"'", '"'}:
            return cleaned_value[1:-1].strip()
    return cleaned_value


def read_config_text(config_path: Path = DEFAULT_CONFIG_PATH) -> str:
    if not config_path.exists():
        return ""

    return config_path.read_text(encoding="utf-8")


def extract_webhook_url_from_text(config_text: str) -> str | None:
    inside_alerts_block = False

    for raw_line in config_text.splitlines():
        line = raw_line.split("#", 1)[0].rstrip()
        if not line.strip():
            continue

        indentation = len(line) - len(line.lstrip(" "))
        stripped_line = line.strip()

        if indentation == 0:
            inside_alerts_block = stripped_line == "alerts:"
            if stripped_line.startswith("google_chat_webhook_url:"):
                _, _, value = stripped_line.partition(":")
                normalized_value = normalize_scalar(value)
                if normalized_value:
                    return normalized_value
            continue

        if inside_alerts_block and stripped_line.startswith("google_chat_webhook_url:"):
            _, _, value = stripped_line.partition(":")
            normalized_value = normalize_scalar(value)
            if normalized_value:
                return normalized_value

        if indentation == 0:
            inside_alerts_block = False

    return None


def resolve_webhook_url(
    config_path: Path = DEFAULT_CONFIG_PATH,
    environ: dict[str, str] | None = None,
) -> str | None:
    environment = environ if environ is not None else os.environ
    env_url = environment.get(WEBHOOK_ENV_VAR, "").strip()
    if env_url:
        return env_url

    config_text = read_config_text(config_path)
    return extract_webhook_url_from_text(config_text)


def build_message_text(
    *,
    pipeline: str,
    status: str,
    run_label: str | None = None,
    step: str | None = None,
    exit_code: int | None = None,
    hostname: str | None = None,
    headless: str | None = None,
    log_file: str | None = None,
    details: str | None = None,
    timestamp: datetime | None = None,
) -> str:
    normalized_status = status.upper()
    pipeline_label = pipeline.upper()
    timestamp = timestamp or datetime.now(timezone.utc)

    lines = [f"[{normalized_status}] {pipeline_label} ETL {normalized_status.lower()}"]

    fields = [
        ("Pipeline", pipeline_label),
        ("Run", run_label),
        ("Step", step),
        ("Exit code", str(exit_code) if exit_code is not None else None),
        ("Host", hostname),
        ("Headless", headless),
        ("Log file", log_file),
        ("Time (UTC)", timestamp.strftime("%Y-%m-%d %H:%M:%S")),
    ]

    for label, value in fields:
        if value:
            lines.append(f"{label}: {value}")

    if details:
        lines.extend(["Details:", details])

    return "\n".join(lines)


def send_google_chat_message(
    webhook_url: str,
    message_text: str,
    timeout_seconds: int = 30,
) -> None:
    payload = json.dumps({"text": message_text}).encode("utf-8")
    chat_request = request.Request(
        webhook_url,
        data=payload,
        headers={"Content-Type": "application/json; charset=UTF-8"},
        method="POST",
    )

    with request.urlopen(chat_request, timeout=timeout_seconds) as response:
        status_code = getattr(response, "status", response.getcode())
        if status_code >= 400:
            raise RuntimeError(
                f"Google Chat webhook returned unexpected status {status_code}"
            )


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Send a Google Chat webhook alert for ETL status.",
    )
    parser.add_argument("--pipeline", required=True)
    parser.add_argument("--status", default="FAILED")
    parser.add_argument("--run-label")
    parser.add_argument("--step")
    parser.add_argument("--exit-code", type=int)
    parser.add_argument("--hostname", default=socket.gethostname())
    parser.add_argument("--headless")
    parser.add_argument("--log-file")
    parser.add_argument("--details")
    parser.add_argument(
        "--config-path",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help="Optional path to config.yaml.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    try:
        webhook_url = resolve_webhook_url(config_path=args.config_path)
    except Exception as exc:
        print(f"Failed to load Google Chat webhook configuration: {exc}", file=sys.stderr)
        return 1

    if not webhook_url:
        print(
            "Google Chat webhook is not configured. Set GOOGLE_CHAT_WEBHOOK_URL "
            "or config.yaml -> alerts.google_chat_webhook_url.",
            file=sys.stderr,
        )
        return 1

    message_text = build_message_text(
        pipeline=args.pipeline,
        status=args.status,
        run_label=args.run_label,
        step=args.step,
        exit_code=args.exit_code,
        hostname=args.hostname,
        headless=args.headless,
        log_file=args.log_file,
        details=args.details,
    )

    try:
        send_google_chat_message(webhook_url, message_text)
    except (error.URLError, RuntimeError) as exc:
        print(f"Failed to send Google Chat alert: {exc}", file=sys.stderr)
        return 1

    print(
        f"Google Chat alert sent for pipeline={args.pipeline} status={args.status}",
        file=sys.stdout,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
