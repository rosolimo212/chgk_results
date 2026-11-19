"""Load sport ChGK results into Postgres, or only check them when write_sql is off.

    python chgk_results.py load-one --id 5022
    python chgk_results.py update
    python chgk_results.py load-all
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Optional

from client import HttpClient
from commands import Echo, Report, run_load_all, run_load_one, run_update, summarize
from config_load import Settings, load_settings
from store import MemoryStore, PostgresStore


def default_config_path() -> Path:
    root = Path(__file__).resolve().parent
    local = root / "config.json"
    if local.exists():
        return local
    return root / "config.example.json"


def parse_args(argv: Optional[list[str]]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Загрузка результатов ЧГК в Postgres")
    parser.add_argument("--config", default=None, help="JSON с write_sql, окном и воркерами")
    subparsers = parser.add_subparsers(dest="command", required=True)
    one = subparsers.add_parser("load-one", help="Один турнир")
    one.add_argument("--id", type=int, required=True)
    subparsers.add_parser("update", help="Окно и турниры, изменённые после прошлой заливки")
    subparsers.add_parser("load-all", help="Полная перезапись с продолжением после остановки")
    return parser.parse_args(argv)


def main(
    argv: Optional[list[str]] = None,
    *,
    client: Any = None,
    store: Any = None,
    settings: Optional[Settings] = None,
    now: str | None = None,
    echo: Echo = print,
) -> int:
    args = parse_args(argv)
    if settings is None:
        try:
            settings = load_settings(args.config or default_config_path())
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            echo(f"конфиг: {exc}")
            return 2
    if client is None:
        client = HttpClient(settings)
    if store is None:
        if settings.database_url:
            store = PostgresStore(settings.database_url, allow_write=settings.write_sql)
        else:
            store = MemoryStore()

    report: Report
    if args.command == "load-one":
        report = run_load_one(
            client,
            store,
            args.id,
            write_sql=settings.write_sql,
            now=now,
            echo=echo,
        )
    elif args.command == "update":
        report = run_update(
            client,
            store,
            write_sql=settings.write_sql,
            window_days=settings.window_days,
            workers=settings.workers,
            now=now,
            echo=echo,
        )
    elif args.command == "load-all":
        report = run_load_all(
            client,
            store,
            write_sql=settings.write_sql,
            workers=settings.workers,
            now=now,
            echo=echo,
        )
    else:
        echo("неизвестная команда")
        return 2
    echo(summarize(report))
    return 1 if report.failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
