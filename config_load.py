"""Load the committable JSON config and the gitignored secrets file."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    write_sql: bool
    window_days: int
    workers: int
    request_delay_seconds: float
    items_per_page: int
    api_base: str
    api_token: str | None
    database_url: str | None
    config_path: Path


def load_simple_yaml(text: str) -> dict[str, str]:
    """Read a flat key: value file. Nested YAML is intentionally unsupported."""
    values: dict[str, str] = {}
    for raw_line in text.splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if not line or ":" not in line:
            continue
        key, value = line.split(":", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def load_settings(path: str | Path) -> Settings:
    config_path = Path(path)
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    write_sql = payload.get("write_sql")
    if not isinstance(write_sql, bool):
        raise ValueError("write_sql must be a JSON boolean")
    window_days = payload.get("window_days")
    workers = payload.get("workers")
    delay = payload.get("request_delay_seconds")
    items_per_page = payload.get("items_per_page")
    api_base = payload.get("api_base")
    if not isinstance(window_days, int) or window_days < 1:
        raise ValueError("window_days must be an integer >= 1")
    if not isinstance(workers, int) or workers < 1:
        raise ValueError("workers must be an integer >= 1")
    if not isinstance(delay, (int, float)) or delay < 0:
        raise ValueError("request_delay_seconds must be a number >= 0")
    if not isinstance(items_per_page, int) or not 1 <= items_per_page <= 500:
        raise ValueError("items_per_page must be an integer from 1 to 500")
    if not isinstance(api_base, str) or not api_base.startswith("http"):
        raise ValueError("api_base must be an http(s) URL")

    secrets: dict[str, str] = {}
    secrets_path = config_path.parent / "secrets.yaml"
    if secrets_path.exists():
        secrets = load_simple_yaml(secrets_path.read_text(encoding="utf-8"))
    token = secrets.get("api_token") or None
    database_url = secrets.get("database_url") or None
    if write_sql and not database_url:
        raise ValueError("write_sql is true but secrets.yaml has no database_url")
    return Settings(
        write_sql=write_sql,
        window_days=window_days,
        workers=workers,
        request_delay_seconds=float(delay),
        items_per_page=items_per_page,
        api_base=api_base.rstrip("/"),
        api_token=token,
        database_url=database_url,
        config_path=config_path,
    )
