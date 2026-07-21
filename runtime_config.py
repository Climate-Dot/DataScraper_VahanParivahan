from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config.yaml"


def get_previous_month_year_label(reference_date: datetime | None = None):
    current_date = reference_date or datetime.now()
    first_day_of_current_month = current_date.replace(day=1)
    last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
    return (
        last_day_of_previous_month.strftime("%b").upper(),
        last_day_of_previous_month.strftime("%Y"),
    )


def resolve_month_year_args(args: Iterable[str] | None = None):
    parsed_args = list(args or [])
    if len(parsed_args) >= 2:
        return parsed_args[0].upper(), str(parsed_args[1])

    return get_previous_month_year_label()


def load_config(config_path: str | Path = DEFAULT_CONFIG_PATH):
    try:
        import yaml
    except ImportError as exc:
        raise RuntimeError(
            "PyYAML is required to load config.yaml in this runtime."
        ) from exc

    path = Path(config_path)
    if not path.is_absolute():
        cwd_candidate = Path.cwd() / path
        project_candidate = PROJECT_ROOT / path
        if cwd_candidate.exists():
            path = cwd_candidate
        else:
            path = project_candidate

    with path.open("r", encoding="utf-8") as config_file:
        payload = yaml.safe_load(config_file) or {}

    if not isinstance(payload, dict):
        raise ValueError("config.yaml must contain a top-level YAML mapping.")

    return payload
