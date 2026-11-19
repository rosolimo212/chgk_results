"""load-one, update and load-all share one tournament pipeline."""

from __future__ import annotations

from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional

from build import Bundle, build_bundle, date_only
from checks import check_bundle, check_response
from client import ApiError

Echo = Callable[[str], None]


@dataclass
class Outcome:
    tournament_id: Optional[int]
    action: str
    errors: list[str] = field(default_factory=list)
    bundle: Optional[Bundle] = None


@dataclass
class Report:
    outcomes: list[Outcome]

    @property
    def failed(self) -> bool:
        return any(item.action == "failed" for item in self.outcomes)


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def summarize(report: Report) -> str:
    counts = Counter(item.action for item in report.outcomes)
    skipped = counts["skipped_hidden"] + counts["skipped_resume"]
    return (
        f"итого: записано={counts['wrote']}, проверено={counts['checked']}, "
        f"пропущено={skipped}, ошибок={counts['failed']}"
    )


def _line(outcome: Outcome) -> str:
    label = {
        "wrote": "записан",
        "checked": "проверен",
        "skipped_hidden": "пропуск, результаты скрыты и маска уже есть",
        "skipped_resume": "пропуск, без изменений",
        "failed": "ошибка",
    }[outcome.action]
    suffix = ""
    if outcome.bundle is not None:
        suffix = f", команды={len(outcome.bundle.teams)}, вопросы={len(outcome.bundle.questions)}"
    errors = f": {'; '.join(outcome.errors)}" if outcome.errors else ""
    return f"{label} {outcome.tournament_id}{suffix}{errors}"


def load_tournament(
    client: Any,
    store: Any,
    tournament_id: int,
    *,
    write_sql: bool,
    inserted_at: str,
    echo: Echo = print,
) -> Outcome:
    try:
        tournament, results, requests_rows = client.fetch(tournament_id)
    except ApiError as exc:
        outcome = Outcome(tournament_id, "failed", [str(exc)])
        echo(_line(outcome))
        return outcome

    response_errors = check_response(tournament, results, requests_rows)
    if response_errors:
        outcome = Outcome(tournament_id, "failed", response_errors)
        echo(_line(outcome))
        return outcome

    bundle = build_bundle(tournament, results, requests_rows, inserted_at=inserted_at)
    data_errors = check_bundle(bundle)
    if data_errors:
        outcome = Outcome(tournament_id, "failed", data_errors, bundle)
        echo(_line(outcome))
        return outcome

    if bundle.hidden and store.has_published_mask(tournament_id):
        outcome = Outcome(tournament_id, "skipped_hidden", [], bundle)
        echo(_line(outcome))
        return outcome

    if write_sql:
        try:
            store.replace_tournament(bundle)
        except Exception as exc:
            outcome = Outcome(tournament_id, "failed", [f"{exc.__class__.__name__}: {exc}"[:300]])
            echo(_line(outcome))
            return outcome
        outcome = Outcome(tournament_id, "wrote", [], bundle)
    else:
        outcome = Outcome(tournament_id, "checked", [], bundle)
    echo(_line(outcome))
    return outcome


def _ids(rows: list[dict[str, Any]]) -> list[int]:
    found: list[int] = []
    for row in rows:
        if isinstance(row, dict) and row.get("id") is not None:
            found.append(int(row["id"]))
    return sorted(set(found))


def _map(ids: list[int], worker: Callable[[int], Outcome], workers: int) -> list[Outcome]:
    if workers <= 1 or len(ids) <= 1:
        return [worker(tournament_id) for tournament_id in ids]
    with ThreadPoolExecutor(max_workers=workers) as pool:
        return list(pool.map(worker, ids))


def run_load_one(
    client: Any,
    store: Any,
    tournament_id: int,
    *,
    write_sql: bool,
    now: str | None = None,
    echo: Echo = print,
) -> Report:
    outcome = load_tournament(
        client,
        store,
        tournament_id,
        write_sql=write_sql,
        inserted_at=now or utc_now(),
        echo=echo,
    )
    return Report([outcome])


def run_update(
    client: Any,
    store: Any,
    *,
    write_sql: bool,
    window_days: int,
    workers: int,
    now: str | None = None,
    today: datetime | None = None,
    echo: Echo = print,
) -> Report:
    inserted_at = now or utc_now()
    day = (today or datetime.now(timezone.utc)).date()
    cutoff = (day - timedelta(days=window_days)).isoformat()
    try:
        by_end = client.list_tournaments({"dateEnd[after]": cutoff})
        found = set(_ids(by_end))
        last_success = store.latest_success_at()
        if last_success:
            by_edit = client.list_tournaments({"lastEditDate[after]": last_success})
            found.update(_ids(by_edit))
    except ApiError as exc:
        outcome = Outcome(None, "failed", [str(exc)])
        echo(_line(outcome))
        return Report([outcome])

    def worker(tournament_id: int) -> Outcome:
        return load_tournament(
            client,
            store,
            tournament_id,
            write_sql=write_sql,
            inserted_at=inserted_at,
            echo=echo,
        )

    return Report(_map(sorted(found), worker, workers))


def run_load_all(
    client: Any,
    store: Any,
    *,
    write_sql: bool,
    workers: int,
    now: str | None = None,
    echo: Echo = print,
) -> Report:
    inserted_at = now or utc_now()
    try:
        listed = client.list_tournaments({"order[id]": "asc"})
    except ApiError as exc:
        outcome = Outcome(None, "failed", [str(exc)])
        echo(_line(outcome))
        return Report([outcome])

    jobs: list[tuple[int, str | None]] = []
    skipped: list[Outcome] = []
    for row in listed:
        if not isinstance(row, dict) or row.get("id") is None:
            continue
        tournament_id = int(row["id"])
        edit = date_only(row.get("lastEditDate"))
        if edit and store.loaded_edit(tournament_id) == edit:
            outcome = Outcome(tournament_id, "skipped_resume", [])
            echo(_line(outcome))
            skipped.append(outcome)
            continue
        jobs.append((tournament_id, edit))

    def worker(job: tuple[int, str | None]) -> Outcome:
        return load_tournament(
            client,
            store,
            job[0],
            write_sql=write_sql,
            inserted_at=inserted_at,
            echo=echo,
        )

    if workers <= 1 or len(jobs) <= 1:
        loaded = [worker(job) for job in jobs]
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            loaded = list(pool.map(worker, jobs))
    return Report(skipped + loaded)
