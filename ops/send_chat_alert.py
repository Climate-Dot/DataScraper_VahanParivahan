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
DEFAULT_EXCERPT_LINE_COUNT = 8
DEFAULT_EXCERPT_CHAR_LIMIT = 1500
INTERESTING_LOG_MARKERS = (
    "ERROR",
    "Error",
    "Traceback",
    "Exception",
    "FAILED",
    "Failed",
    "Access Forbidden",
    "blocked",
    "Timeout",
    "KeyError",
    "FileNotFoundError",
    "ValueError",
)
STATUS_HEADERS = {
    "FAILED": "[FAILED] {pipeline} ETL pipeline",
    "SUCCESS": "🎉 [SUCCESS] {pipeline} ETL pipeline",
    "TEST": "🧪 [TEST] {pipeline} ETL pipeline",
}
STATUS_STEP_LABELS = {
    "FAILED": "Failed step",
    "SUCCESS": "Final step",
    "TEST": "Step",
}
STATUS_DEFAULT_DETAILS = {
    "SUCCESS": "Everything finished cleanly. The data gremlins stayed off duty this run.",
}
STATUSES_WITH_LOG_EXCERPTS = {"FAILED"}


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


def normalize_headless_label(headless: str | None) -> str | None:
    if headless is None:
        return None

    normalized = headless.strip().lower()
    if not normalized:
        return None

    if normalized in {"0", "false", "no", "off"}:
        return "headed (xvfb)"
    if normalized in {"1", "true", "yes", "on"}:
        return "headless"
    return headless


def build_recent_excerpt(
    file_path: str | None,
    *,
    max_lines: int = DEFAULT_EXCERPT_LINE_COUNT,
    max_chars: int = DEFAULT_EXCERPT_CHAR_LIMIT,
) -> str | None:
    if not file_path:
        return None

    path = Path(file_path)
    if not path.exists() or not path.is_file():
        return None

    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return None

    non_empty_lines = [line.rstrip() for line in lines if line.strip()]
    if not non_empty_lines:
        return None

    interesting_lines = [
        line
        for line in non_empty_lines
        if any(marker in line for marker in INTERESTING_LOG_MARKERS)
    ]
    selected_lines = interesting_lines[-max_lines:] or non_empty_lines[-max_lines:]
    excerpt = "\n".join(selected_lines).strip()

    if len(excerpt) <= max_chars:
        return excerpt

    truncated_excerpt = excerpt[-max_chars:].lstrip()
    return f"...\n{truncated_excerpt}"


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
    details_file: str | None = None,
    log_excerpt: str | None = None,
    details_excerpt: str | None = None,
    timestamp: datetime | None = None,
) -> str:
    normalized_status = status.upper()
    pipeline_label = pipeline.upper()
    timestamp = timestamp or datetime.now(timezone.utc)
    header_template = STATUS_HEADERS.get(
        normalized_status, "[{status}] {pipeline} ETL pipeline"
    )
    step_label = STATUS_STEP_LABELS.get(normalized_status, "Step")
    resolved_details = details or STATUS_DEFAULT_DETAILS.get(normalized_status)

    lines = [
        header_template.format(status=normalized_status, pipeline=pipeline_label)
    ]
    lines.extend(
        [
            "Summary:",
            f"- Run: {run_label or 'unknown'}",
            f"- {step_label}: {step or 'unknown'}",
            f"- Exit code: {exit_code if exit_code is not None else 'unknown'}",
            f"- Host: {hostname or 'unknown'}",
            f"- Browser mode: {normalize_headless_label(headless) or 'unknown'}",
            f"- Time (UTC): {timestamp.strftime('%Y-%m-%d %H:%M:%S')}",
        ]
    )

    if log_file or details_file:
        lines.append("")
        lines.append("Artifacts:")
        if log_file:
            lines.append(f"- ETL log: {log_file}")
        if details_file:
            lines.append(f"- Extra log: {details_file}")

    if resolved_details:
        lines.append("")
        lines.append("Context:")
        lines.append(resolved_details)

    if log_excerpt:
        lines.append("")
        lines.append("Recent ETL log lines:")
        lines.append(log_excerpt)

    if details_excerpt:
        lines.append("")
        lines.append("Recent extra log lines:")
        lines.append(details_excerpt)

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
    parser.add_argument("--details-file")
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

    normalized_status = args.status.upper()
    include_log_excerpts = normalized_status in STATUSES_WITH_LOG_EXCERPTS

    message_text = build_message_text(
        pipeline=args.pipeline,
        status=normalized_status,
        run_label=args.run_label,
        step=args.step,
        exit_code=args.exit_code,
        hostname=args.hostname,
        headless=args.headless,
        log_file=args.log_file,
        details=args.details,
        details_file=args.details_file,
        log_excerpt=build_recent_excerpt(args.log_file) if include_log_excerpts else None,
        details_excerpt=build_recent_excerpt(args.details_file)
        if include_log_excerpts
        else None,
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
