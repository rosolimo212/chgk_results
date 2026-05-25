"""Download and update sport ChGK tournament data from rating.chgk.info API."""

from __future__ import annotations

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Optional
from urllib.parse import urljoin

import pandas as pd
import requests

API_BASE = "https://api.rating.chgk.info"
ITEMS_PER_PAGE = 500
DEFAULT_WINDOW_DAYS = 30
DEFAULT_WORKERS = 3
DEFAULT_MAX_RETRIES = 5
DEFAULT_TIMEOUT = 30
DEFAULT_REQUEST_DELAY = 0.1

RESULTS_PARAMS = {
    "includeTeamMembers": "1",
    "includeMasksAndControversials": "1",
    "includeRatingB": "1",
}

TEAM_PERFORMANCE_COLUMNS = [
    "tournament_id",
    "team_id",
    "team_name",
    "current_team_name",
    "position",
    "questions_total",
    "teams_count",
    "is_synch",
    "venue_id",
    "venue_name",
    "town_name",
    "team_played_date",
    "in_rating",
    "rg",
    "r",
    "rb",
    "rt",
    "b",
    "d",
    "d1",
    "d2",
    "bp",
    "predicted_position",
    "flags",
    "tournament_name",
    "tournament_long_name",
    "date_start",
    "date_end",
    "last_edit_date",
    "type_id",
    "type_name",
    "season_id",
    "difficulty_forecast",
    "true_dl",
    "rating_systems",
    "has_chgk_rating",
    "town_id",
    "editors_ids",
    "orgcommittee_ids",
    "fetched_at",
]

PLAYER_PERFORMANCE_COLUMNS = [
    "tournament_id",
    "team_id",
    "player_id",
    "player_name",
    "player_surname",
    "player_patronymic",
    "flag",
    "player_rating",
    "fetched_at",
]

QUESTION_RESULT_COLUMNS = [
    "tournament_id",
    "team_id",
    "question_number",
    "mask_char",
    "taken",
    "fetched_at",
]

INDEX_COLUMNS = [
    "tournament_id",
    "tournament_name",
    "date_start",
    "date_end",
    "last_edit_date",
    "type_name",
    "season_id",
    "rating_systems",
    "has_chgk_rating",
    "fetch_status",
    "fetched_at",
    "error_message",
]

_FAILURE_COLUMNS = [
    "timestamp",
    "entity_type",
    "entity_id",
    "url",
    "status_code",
    "error_message",
]

_write_lock = threading.Lock()


def parse_iso_date(value: Any) -> Optional[str]:
    """Convert API datetime string to yyyy-mm-dd."""
    if value is None or value == "":
        return None
    text = str(value)
    if "T" in text:
        text = text.split("T", 1)[0]
    return text[:10]


def parse_mask(mask: Optional[str]) -> list[tuple[int, str, int]]:
    """Expand result mask into per-question rows."""
    if not mask:
        return []
    rows: list[tuple[int, str, int]] = []
    for index, char in enumerate(mask, start=1):
        taken = 1 if char == "1" else 0
        rows.append((index, char, taken))
    return rows


def _player_ids(players: Any) -> str:
    if not players:
        return ""
    ids: list[str] = []
    for player in players:
        if isinstance(player, dict) and player.get("id") is not None:
            ids.append(str(player["id"]))
    return ",".join(ids)


def _serialize_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        parts: list[str] = []
        for item in value:
            if isinstance(item, dict):
                if "name" in item:
                    parts.append(str(item["name"]))
                elif "id" in item:
                    parts.append(str(item["id"]))
                else:
                    parts.append(json.dumps(item, ensure_ascii=False))
            else:
                parts.append(str(item))
        return ",".join(parts)
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False)
    return str(value)
    if not players:
        return ""
    ids: list[str] = []
    for player in players:
        if isinstance(player, dict) and player.get("id") is not None:
            ids.append(str(player["id"]))
    return ",".join(ids)


def _rating_systems_text(value: Any) -> str:
    if not value:
        return ""
    if isinstance(value, str):
        return value
    return ",".join(str(item) for item in value)


def _has_chgk_rating(rating_systems: Any) -> bool:
    systems = rating_systems or []
    if isinstance(systems, str):
        systems = [part.strip() for part in systems.split(",") if part.strip()]
    return "chgkgg" in systems


def flatten_tournament_meta(tournament: dict[str, Any]) -> dict[str, Any]:
    """Extract tournament-level fields used in team_performances."""
    tournament_type = tournament.get("type") or {}
    rating_systems = tournament.get("ratingSystems")
    return {
        "tournament_id": tournament.get("id"),
        "tournament_name": tournament.get("name"),
        "tournament_long_name": tournament.get("longName"),
        "date_start": parse_iso_date(tournament.get("dateStart")),
        "date_end": parse_iso_date(tournament.get("dateEnd")),
        "last_edit_date": parse_iso_date(tournament.get("lastEditDate")),
        "type_id": tournament_type.get("id"),
        "type_name": tournament_type.get("name"),
        "season_id": tournament.get("idseason"),
        "difficulty_forecast": tournament.get("difficultyForecast"),
        "true_dl": tournament.get("trueDL"),
        "rating_systems": _rating_systems_text(rating_systems),
        "has_chgk_rating": _has_chgk_rating(rating_systems),
        "town_id": tournament.get("idtown"),
        "editors_ids": _player_ids(tournament.get("editors")),
        "orgcommittee_ids": _player_ids(tournament.get("orgcommittee")),
    }


def _team_played_date(
    result: dict[str, Any],
    tournament_meta: dict[str, Any],
) -> Optional[str]:
    synch_request = result.get("synchRequest") or {}
    if synch_request.get("dateStart"):
        return parse_iso_date(synch_request["dateStart"])
    return tournament_meta.get("date_start")


def flatten_team_result(
    tournament_meta: dict[str, Any],
    result: dict[str, Any],
    *,
    teams_count: int,
    fetched_at: str,
) -> dict[str, Any]:
    """Build one team_performances row."""
    team = result.get("team") or {}
    current = result.get("current") or {}
    rating = result.get("rating") or {}
    synch_request = result.get("synchRequest") or {}
    venue = synch_request.get("venue") or {}
    town = synch_request.get("town") or venue.get("town") or {}

    row = dict(tournament_meta)
    row.update(
        {
            "team_id": team.get("id"),
            "team_name": team.get("name"),
            "current_team_name": current.get("name"),
            "position": result.get("position"),
            "questions_total": result.get("questionsTotal"),
            "teams_count": teams_count,
            "is_synch": bool(synch_request),
            "venue_id": venue.get("id"),
            "venue_name": venue.get("name"),
            "town_name": town.get("name") if isinstance(town, dict) else None,
            "team_played_date": _team_played_date(result, tournament_meta),
            "in_rating": rating.get("inRating"),
            "rg": rating.get("rg"),
            "r": rating.get("r"),
            "rb": rating.get("rb"),
            "rt": rating.get("rt"),
            "b": rating.get("b"),
            "d": rating.get("d"),
            "d1": rating.get("d1"),
            "d2": rating.get("d2"),
            "bp": rating.get("bp"),
            "predicted_position": rating.get("predictedPosition"),
            "flags": _serialize_value(result.get("flags")),
            "fetched_at": fetched_at,
        }
    )
    return row


def flatten_player_results(
    tournament_id: int,
    result: dict[str, Any],
    *,
    fetched_at: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    team_id = (result.get("team") or {}).get("id")
    for member in result.get("teamMembers") or []:
        player = member.get("player") or {}
        rows.append(
            {
                "tournament_id": tournament_id,
                "team_id": team_id,
                "player_id": player.get("id"),
                "player_name": player.get("name"),
                "player_surname": player.get("surname"),
                "player_patronymic": player.get("patronymic"),
                "flag": member.get("flag"),
                "player_rating": member.get("rating"),
                "fetched_at": fetched_at,
            }
        )
    return rows


def flatten_question_results(
    tournament_id: int,
    result: dict[str, Any],
    *,
    fetched_at: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    team_id = (result.get("team") or {}).get("id")
    for question_number, mask_char, taken in parse_mask(result.get("mask")):
        rows.append(
            {
                "tournament_id": tournament_id,
                "team_id": team_id,
                "question_number": question_number,
                "mask_char": mask_char,
                "taken": taken,
                "fetched_at": fetched_at,
            }
        )
    return rows


def flatten_tournament_results(
    tournament: dict[str, Any],
    results: list[dict[str, Any]],
    *,
    fetched_at: Optional[str] = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Convert tournament meta and results payload into three table row lists."""
    fetched_at = fetched_at or _utc_now_iso()
    tournament_meta = flatten_tournament_meta(tournament)
    teams_count = len(results)

    team_rows: list[dict[str, Any]] = []
    player_rows: list[dict[str, Any]] = []
    question_rows: list[dict[str, Any]] = []

    tournament_id = tournament_meta["tournament_id"]
    for result in results:
        team_rows.append(
            flatten_team_result(
                tournament_meta,
                result,
                teams_count=teams_count,
                fetched_at=fetched_at,
            )
        )
        player_rows.extend(
            flatten_player_results(tournament_id, result, fetched_at=fetched_at)
        )
        question_rows.extend(
            flatten_question_results(tournament_id, result, fetched_at=fetched_at)
        )

    return team_rows, player_rows, question_rows


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _today() -> date:
    return datetime.now(timezone.utc).date()


def _data_paths(data_dir: str | Path) -> dict[str, Path]:
    root = Path(data_dir)
    return {
        "root": root,
        "state": root / "state",
        "tables": root / "tables",
        "team_shards": root / "tables" / "team_performances",
        "player_shards": root / "tables" / "player_performances",
        "question_shards": root / "tables" / "question_results",
        "failures": root / "failures",
        "sync_state": root / "state" / "sync_state.json",
        "tournaments_index": root / "state" / "tournaments_index.parquet",
        "fetch_failures": root / "failures" / "fetch_failures.csv",
        "list_pages_failed": root / "failures" / "list_pages_failed.csv",
    }


def _ensure_layout(data_dir: str | Path) -> dict[str, Path]:
    paths = _data_paths(data_dir)
    for key in (
        "state",
        "tables",
        "team_shards",
        "player_shards",
        "question_shards",
        "failures",
    ):
        paths[key].mkdir(parents=True, exist_ok=True)
    return paths


def _load_sync_state(paths: dict[str, Path]) -> dict[str, Any]:
    if not paths["sync_state"].exists():
        return {}
    with paths["sync_state"].open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _save_sync_state(paths: dict[str, Path], state: dict[str, Any]) -> None:
    paths["state"].mkdir(parents=True, exist_ok=True)
    with paths["sync_state"].open("w", encoding="utf-8") as handle:
        json.dump(state, handle, ensure_ascii=False, indent=2)


def _append_csv(path: Path, row: dict[str, Any], columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame([row], columns=columns)
    write_header = not path.exists()
    frame.to_csv(path, mode="a", header=write_header, index=False)


def _log_failure(
    paths: dict[str, Path],
    *,
    entity_type: str,
    entity_id: Any,
    url: str,
    status_code: Optional[int],
    error_message: str,
) -> None:
    with _write_lock:
        _append_csv(
            paths["fetch_failures"],
            {
                "timestamp": _utc_now_iso(),
                "entity_type": entity_type,
                "entity_id": entity_id,
                "url": url,
                "status_code": status_code,
                "error_message": error_message,
            },
            _FAILURE_COLUMNS,
        )


def _log_list_page_failure(
    paths: dict[str, Path],
    *,
    page: int,
    url: str,
    status_code: Optional[int],
    error_message: str,
) -> None:
    with _write_lock:
        _append_csv(
            paths["list_pages_failed"],
            {
                "timestamp": _utc_now_iso(),
                "entity_type": "tournament_list_page",
                "entity_id": page,
                "url": url,
                "status_code": status_code,
                "error_message": error_message,
            },
            _FAILURE_COLUMNS,
        )


def _extract_collection(payload: Any) -> list[Any]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        if "member" in payload:
            return payload["member"]
        if "hydra:member" in payload:
            return payload["hydra:member"]
    return []


def api_request(
    path: str,
    *,
    params: Optional[dict[str, Any]] = None,
    is_verbose: bool = False,
    max_retries: int = DEFAULT_MAX_RETRIES,
    timeout: int = DEFAULT_TIMEOUT,
    request_delay: float = DEFAULT_REQUEST_DELAY,
) -> tuple[Optional[Any], Optional[str], Optional[int]]:
    """Perform GET request with retries. Returns (data, error, status_code)."""
    url = urljoin(API_BASE + "/", path.lstrip("/"))
    headers = {"accept": "application/json"}
    last_error: Optional[str] = None
    last_status: Optional[int] = None

    for attempt in range(1, max_retries + 1):
        try:
            if request_delay:
                time.sleep(request_delay)
            response = requests.get(url, params=params, headers=headers, timeout=timeout)
            last_status = response.status_code
            if is_verbose:
                print(f"GET {response.url} -> {response.status_code} (attempt {attempt})")
            if response.status_code == 200:
                return response.json(), None, response.status_code
            last_error = response.text[:500]
            if response.status_code in {429, 500, 502, 503, 504}:
                time.sleep(min(2 ** attempt, 30))
                continue
            return None, last_error, response.status_code
        except requests.RequestException as exc:
            last_error = str(exc)
            if is_verbose:
                print(f"GET {url} failed: {exc} (attempt {attempt})")
            time.sleep(min(2 ** attempt, 30))

    return None, last_error, last_status


def fetch_tournament_list_page(
    page: int,
    *,
    params: Optional[dict[str, Any]] = None,
    is_verbose: bool = False,
) -> tuple[list[dict[str, Any]], Optional[str], Optional[int], str]:
    query = {"itemsPerPage": ITEMS_PER_PAGE, "page": page}
    if params:
        query.update(params)
    path = "tournaments.json"
    data, error, status = api_request(path, params=query, is_verbose=is_verbose)
    url = urljoin(API_BASE + "/", path)
    return _extract_collection(data), error, status, url


def iter_tournament_list(
    *,
    params: Optional[dict[str, Any]] = None,
    is_verbose: bool = False,
    paths: Optional[dict[str, Path]] = None,
) -> list[dict[str, Any]]:
    """Download full tournament list with pagination."""
    tournaments: list[dict[str, Any]] = []
    page = 1
    while True:
        page_items, error, status, url = fetch_tournament_list_page(
            page,
            params=params,
            is_verbose=is_verbose,
        )
        if error:
            if paths is not None:
                _log_list_page_failure(
                    paths,
                    page=page,
                    url=url,
                    status_code=status,
                    error_message=error,
                )
            if is_verbose:
                print(f"Stopped tournament list at page {page}: {error}")
            break
        if not page_items:
            break
        tournaments.extend(page_items)
        if is_verbose:
            print(f"Tournament list page {page}: {len(page_items)} items")
        if len(page_items) < ITEMS_PER_PAGE:
            break
        page += 1
    return tournaments


def fetch_team_tournament_ids(
    team_id: int,
    *,
    is_verbose: bool = False,
    paths: Optional[dict[str, Path]] = None,
) -> set[int]:
    tournament_ids: set[int] = set()
    page = 1
    while True:
        path = f"teams/{team_id}/tournaments.json"
        data, error, status = api_request(
            path,
            params={"itemsPerPage": ITEMS_PER_PAGE, "page": page},
            is_verbose=is_verbose,
        )
        if error:
            if paths is not None:
                _log_failure(
                    paths,
                    entity_type="team_tournaments",
                    entity_id=team_id,
                    url=urljoin(API_BASE + "/", path),
                    status_code=status,
                    error_message=f"page={page}: {error}",
                )
            break
        items = _extract_collection(data)
        if not items:
            break
        for item in items:
            tournament_id = item.get("idtournament")
            if tournament_id is not None:
                tournament_ids.add(int(tournament_id))
        if len(items) < ITEMS_PER_PAGE:
            break
        page += 1
    return tournament_ids


def fetch_tournament_bundle(
    tournament_id: int,
    *,
    is_verbose: bool = False,
) -> tuple[Optional[dict[str, Any]], Optional[list[dict[str, Any]]], Optional[str]]:
    tournament, error, _ = api_request(
        f"tournaments/{tournament_id}.json",
        is_verbose=is_verbose,
    )
    if error or not tournament:
        return None, None, error or "empty tournament payload"

    results, error, _ = api_request(
        f"tournaments/{tournament_id}/results.json",
        params=RESULTS_PARAMS,
        is_verbose=is_verbose,
    )
    if error:
        return tournament, None, error

    result_items = _extract_collection(results)
    if results is not None and not result_items and not isinstance(results, list):
        return tournament, None, "unexpected results payload"

    return tournament, result_items, None


def _write_shard(path: Path, rows: list[dict[str, Any]], columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(rows, columns=columns)
    frame.to_parquet(path, index=False)


def _write_tournament_tables(
    paths: dict[str, Path],
    tournament_id: int,
    team_rows: list[dict[str, Any]],
    player_rows: list[dict[str, Any]],
    question_rows: list[dict[str, Any]],
) -> None:
    _write_shard(
        paths["team_shards"] / f"{tournament_id}.parquet",
        team_rows,
        TEAM_PERFORMANCE_COLUMNS,
    )
    _write_shard(
        paths["player_shards"] / f"{tournament_id}.parquet",
        player_rows,
        PLAYER_PERFORMANCE_COLUMNS,
    )
    _write_shard(
        paths["question_shards"] / f"{tournament_id}.parquet",
        question_rows,
        QUESTION_RESULT_COLUMNS,
    )


def _load_index(paths: dict[str, Path]) -> pd.DataFrame:
    if not paths["tournaments_index"].exists():
        return pd.DataFrame(columns=INDEX_COLUMNS)
    return pd.read_parquet(paths["tournaments_index"])


def _save_index(paths: dict[str, Path], index: pd.DataFrame) -> None:
    paths["state"].mkdir(parents=True, exist_ok=True)
    index.to_parquet(paths["tournaments_index"], index=False)


def _upsert_index_row(
    index: pd.DataFrame,
    row: dict[str, Any],
) -> pd.DataFrame:
    tournament_id = row["tournament_id"]
    if not index.empty and tournament_id in index["tournament_id"].values:
        index = index[index["tournament_id"] != tournament_id]
    return pd.concat([index, pd.DataFrame([row])], ignore_index=True)


def _index_row_from_tournament(
    tournament: dict[str, Any],
    *,
    fetch_status: str,
    fetched_at: str,
    error_message: Optional[str] = None,
) -> dict[str, Any]:
    meta = flatten_tournament_meta(tournament)
    return {
        "tournament_id": meta["tournament_id"],
        "tournament_name": meta["tournament_name"],
        "date_start": meta["date_start"],
        "date_end": meta["date_end"],
        "last_edit_date": meta["last_edit_date"],
        "type_name": meta["type_name"],
        "season_id": meta["season_id"],
        "rating_systems": meta["rating_systems"],
        "has_chgk_rating": meta["has_chgk_rating"],
        "fetch_status": fetch_status,
        "fetched_at": fetched_at,
        "error_message": error_message,
    }


def _should_skip_tournament(
    tournament_id: int,
    last_edit_date: Optional[str],
    index: pd.DataFrame,
    *,
    resume: bool,
) -> bool:
    if not resume or index.empty:
        return False
    rows = index[index["tournament_id"] == tournament_id]
    if rows.empty:
        return False
    row = rows.iloc[-1]
    if row.get("fetch_status") != "ok":
        return False
    if last_edit_date and row.get("last_edit_date") == last_edit_date:
        return True
    return False


def _update_index_row(
    paths: dict[str, Path],
    row: dict[str, Any],
) -> None:
    with _write_lock:
        index = _load_index(paths)
        index = _upsert_index_row(index, row)
        _save_index(paths, index)


def download_tournament(
    tournament_id: int,
    paths: dict[str, Path],
    *,
    is_verbose: bool = False,
    resume: bool = True,
) -> str:
    """Download one tournament and write shard parquet files. Returns status."""
    fetched_at = _utc_now_iso()

    tournament, results, error = fetch_tournament_bundle(
        tournament_id,
        is_verbose=is_verbose,
    )
    if tournament is None:
        _log_failure(
            paths,
            entity_type="tournament",
            entity_id=tournament_id,
            url=f"{API_BASE}/tournaments/{tournament_id}.json",
            status_code=None,
            error_message=error or "missing tournament",
        )
        return "failed"

    last_edit_date = parse_iso_date(tournament.get("lastEditDate"))
    if resume:
        index = _load_index(paths)
        if _should_skip_tournament(tournament_id, last_edit_date, index, resume=True):
            if is_verbose:
                print(f"Skip tournament {tournament_id}: unchanged")
            return "skipped"

    if results is None:
        _log_failure(
            paths,
            entity_type="tournament_results",
            entity_id=tournament_id,
            url=f"{API_BASE}/tournaments/{tournament_id}/results.json",
            status_code=None,
            error_message=error or "missing results",
        )
        _update_index_row(
            paths,
            _index_row_from_tournament(
                tournament,
                fetch_status="results_failed",
                fetched_at=fetched_at,
                error_message=error,
            ),
        )
        return "results_failed"

    team_rows, player_rows, question_rows = flatten_tournament_results(
        tournament,
        results,
        fetched_at=fetched_at,
    )
    _write_tournament_tables(paths, tournament_id, team_rows, player_rows, question_rows)

    _update_index_row(
        paths,
        _index_row_from_tournament(
            tournament,
            fetch_status="ok",
            fetched_at=fetched_at,
        ),
    )
    return "ok"


def _download_many_tournaments(
    tournament_ids: Iterable[int],
    paths: dict[str, Path],
    *,
    is_verbose: bool = False,
    resume: bool = True,
    workers: int = DEFAULT_WORKERS,
) -> dict[str, int]:
    ids = sorted(set(int(tournament_id) for tournament_id in tournament_ids))
    stats = {"total": len(ids), "ok": 0, "skipped": 0, "failed": 0, "results_failed": 0}
    if not ids:
        return stats

    def _worker(tournament_id: int) -> tuple[int, str]:
        status = download_tournament(
            tournament_id,
            paths,
            is_verbose=is_verbose,
            resume=resume,
        )
        return tournament_id, status

    if workers <= 1:
        for tournament_id in ids:
            _, status = _worker(tournament_id)
            stats[status] = stats.get(status, 0) + 1
            if is_verbose:
                print(f"Tournament {tournament_id}: {status}")
        return stats

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_worker, tournament_id): tournament_id for tournament_id in ids}
        for future in as_completed(futures):
            tournament_id = futures[future]
            try:
                _, status = future.result()
            except Exception as exc:
                _log_failure(
                    paths,
                    entity_type="tournament",
                    entity_id=tournament_id,
                    url=f"{API_BASE}/tournaments/{tournament_id}.json",
                    status_code=None,
                    error_message=str(exc),
                )
                status = "failed"
            stats[status] = stats.get(status, 0) + 1
            if is_verbose:
                print(f"Tournament {tournament_id}: {status}")

    return stats


def _index_from_list_items(items: list[dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for item in items:
        meta = flatten_tournament_meta(item)
        rows.append(
            {
                "tournament_id": meta["tournament_id"],
                "tournament_name": meta["tournament_name"],
                "date_start": meta["date_start"],
                "date_end": meta["date_end"],
                "last_edit_date": meta["last_edit_date"],
                "type_name": meta["type_name"],
                "season_id": meta["season_id"],
                "rating_systems": meta["rating_systems"],
                "has_chgk_rating": meta["has_chgk_rating"],
                "fetch_status": "listed",
                "fetched_at": None,
                "error_message": None,
            }
        )
    return pd.DataFrame(rows, columns=INDEX_COLUMNS)


def download_all(
    data_dir: str | Path,
    *,
    is_verbose: bool = False,
    resume: bool = True,
    workers: int = DEFAULT_WORKERS,
) -> dict[str, Any]:
    """Download all tournaments and write parquet shards."""
    paths = _ensure_layout(data_dir)
    list_items = iter_tournament_list(is_verbose=is_verbose, paths=paths)
    list_index = _index_from_list_items(list_items)

    existing_index = _load_index(paths)
    if not existing_index.empty and resume:
        listed_ids = set(list_index["tournament_id"].tolist())
        preserved = existing_index[~existing_index["tournament_id"].isin(listed_ids)]
        index = pd.concat([preserved, list_index], ignore_index=True)
    else:
        index = list_index
    _save_index(paths, index)

    tournament_ids = list_index["tournament_id"].dropna().astype(int).tolist()
    stats = _download_many_tournaments(
        tournament_ids,
        paths,
        is_verbose=is_verbose,
        resume=resume,
        workers=workers,
    )

    state = _load_sync_state(paths)
    state.update(
        {
            "last_success_at": _utc_now_iso(),
            "mode": "download_all",
            "stats": stats,
        }
    )
    _save_sync_state(paths, state)
    if is_verbose:
        print(f"download_all finished: {stats}")
    return stats


def _parse_index_date(value: Any) -> Optional[date]:
    parsed = parse_iso_date(value)
    if not parsed:
        return None
    return date.fromisoformat(parsed)


def select_tournament_ids_for_window(
    index: pd.DataFrame,
    *,
    window_days: int = DEFAULT_WINDOW_DAYS,
    last_sync_at: Optional[str] = None,
) -> set[int]:
    """Select tournaments to refresh by date_end window and last_edit_date."""
    if index.empty:
        return set()

    selected: set[int] = set()
    today = _today()
    date_end_cutoff = today - timedelta(days=window_days)

    for _, row in index.iterrows():
        tournament_id = int(row["tournament_id"])
        date_end = _parse_index_date(row.get("date_end"))
        if date_end is not None and date_end >= date_end_cutoff:
            selected.add(tournament_id)

    if last_sync_at:
        sync_day = _parse_index_date(last_sync_at)
        if sync_day:
            edit_cutoff = sync_day - timedelta(days=1)
            for _, row in index.iterrows():
                last_edit = _parse_index_date(row.get("last_edit_date"))
                if last_edit is not None and last_edit >= edit_cutoff:
                    selected.add(int(row["tournament_id"]))

    return selected


def update_by_window(
    data_dir: str | Path,
    window_days: int = DEFAULT_WINDOW_DAYS,
    *,
    is_verbose: bool = False,
    resume: bool = False,
    workers: int = DEFAULT_WORKERS,
) -> dict[str, Any]:
    """Refresh recent tournaments and tournaments changed since last sync."""
    paths = _ensure_layout(data_dir)
    state = _load_sync_state(paths)
    last_sync_at = state.get("last_success_at")

    recent_end = (_today() - timedelta(days=window_days)).isoformat()
    recent_items = iter_tournament_list(
        params={"dateEnd[after]": recent_end},
        is_verbose=is_verbose,
        paths=paths,
    )

    changed_items: list[dict[str, Any]] = []
    if last_sync_at:
        changed_items = iter_tournament_list(
            params={"lastEditDate[after]": parse_iso_date(last_sync_at)},
            is_verbose=is_verbose,
            paths=paths,
        )

    combined = {item["id"]: item for item in recent_items + changed_items if item.get("id") is not None}
    list_index = _index_from_list_items(list(combined.values()))

    existing_index = _load_index(paths)
    if not existing_index.empty:
        listed_ids = set(list_index["tournament_id"].tolist())
        preserved = existing_index[~existing_index["tournament_id"].isin(listed_ids)]
        index = pd.concat([preserved, list_index], ignore_index=True)
    else:
        index = list_index
    _save_index(paths, index)

    tournament_ids = select_tournament_ids_for_window(
        list_index,
        window_days=window_days,
        last_sync_at=last_sync_at,
    )
    stats = _download_many_tournaments(
        tournament_ids,
        paths,
        is_verbose=is_verbose,
        resume=resume,
        workers=workers,
    )

    state.update(
        {
            "last_success_at": _utc_now_iso(),
            "mode": "update_by_window",
            "window_days": window_days,
            "stats": stats,
        }
    )
    _save_sync_state(paths, state)
    if is_verbose:
        print(f"update_by_window finished: {stats}")
    return stats


def download_teams(
    data_dir: str | Path,
    team_ids: Iterable[int],
    *,
    is_verbose: bool = False,
    resume: bool = True,
    workers: int = DEFAULT_WORKERS,
) -> dict[str, Any]:
    """Download full tournaments for all tournaments where given teams played."""
    paths = _ensure_layout(data_dir)
    tournament_ids: set[int] = set()
    for team_id in team_ids:
        tournament_ids.update(
            fetch_team_tournament_ids(int(team_id), is_verbose=is_verbose, paths=paths)
        )

    stats = _download_many_tournaments(
        tournament_ids,
        paths,
        is_verbose=is_verbose,
        resume=resume,
        workers=workers,
    )

    state = _load_sync_state(paths)
    state.update(
        {
            "last_success_at": _utc_now_iso(),
            "mode": "download_teams",
            "team_ids": [int(team_id) for team_id in team_ids],
            "stats": stats,
        }
    )
    _save_sync_state(paths, state)
    if is_verbose:
        print(f"download_teams finished: {stats}")
    return stats


def retry_failures(
    data_dir: str | Path,
    *,
    is_verbose: bool = False,
    workers: int = DEFAULT_WORKERS,
) -> dict[str, Any]:
    """Retry tournament downloads listed in fetch_failures.csv."""
    paths = _ensure_layout(data_dir)
    if not paths["fetch_failures"].exists():
        return {"total": 0, "ok": 0, "skipped": 0, "failed": 0, "results_failed": 0}

    failures = pd.read_csv(paths["fetch_failures"])
    tournament_ids: set[int] = set()
    for _, row in failures.iterrows():
        if row.get("entity_type") in {"tournament", "tournament_results"}:
            tournament_ids.add(int(row["entity_id"]))

    stats = _download_many_tournaments(
        tournament_ids,
        paths,
        is_verbose=is_verbose,
        resume=False,
        workers=workers,
    )
    if is_verbose:
        print(f"retry_failures finished: {stats}")
    return stats


def _empty_table(columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame({column: pd.Series(dtype="object") for column in columns})


def _prepare_frame_for_concat(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    prepared = frame.reindex(columns=columns).copy()
    for column in columns:
        if column not in frame.columns or prepared[column].isna().all():
            prepared[column] = prepared[column].astype("object")
    return prepared


def _merge_shards(shard_files: list[Path], columns: list[str]) -> pd.DataFrame:
    if not shard_files:
        return _empty_table(columns)

    frames = [
        _prepare_frame_for_concat(pd.read_parquet(path), columns)
        for path in shard_files
    ]
    non_empty = [frame for frame in frames if not frame.empty]
    if not non_empty:
        return _empty_table(columns)
    if len(non_empty) == 1:
        return non_empty[0].reset_index(drop=True)
    return pd.concat(non_empty, ignore_index=True, sort=False)


def compact_tables(data_dir: str | Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Merge per-tournament parquet shards into single parquet files.

    Returns:
        team_performances, player_performances, question_results DataFrames.
    """
    paths = _ensure_layout(data_dir)

    team_df = _merge_shards(
        sorted(paths["team_shards"].glob("*.parquet")),
        TEAM_PERFORMANCE_COLUMNS,
    )
    player_df = _merge_shards(
        sorted(paths["player_shards"].glob("*.parquet")),
        PLAYER_PERFORMANCE_COLUMNS,
    )
    question_df = _merge_shards(
        sorted(paths["question_shards"].glob("*.parquet")),
        QUESTION_RESULT_COLUMNS,
    )

    team_df.to_parquet(paths["tables"] / "team_performances.parquet", index=False)
    player_df.to_parquet(paths["tables"] / "player_performances.parquet", index=False)
    question_df.to_parquet(paths["tables"] / "question_results.parquet", index=False)

    return team_df, player_df, question_df


def get_sync_stats(data_dir: str | Path) -> dict[str, Any]:
    """Return summary information about local data."""
    paths = _ensure_layout(data_dir)
    index = _load_index(paths)
    state = _load_sync_state(paths)

    stats = {
        "tournaments_indexed": int(len(index)),
        "tournaments_ok": int((index["fetch_status"] == "ok").sum()) if not index.empty else 0,
        "team_shards": len(list(paths["team_shards"].glob("*.parquet"))),
        "player_shards": len(list(paths["player_shards"].glob("*.parquet"))),
        "question_shards": len(list(paths["question_shards"].glob("*.parquet"))),
        "sync_state": state,
    }
    if paths["fetch_failures"].exists():
        stats["failure_rows"] = int(len(pd.read_csv(paths["fetch_failures"])))
    else:
        stats["failure_rows"] = 0
    if paths["list_pages_failed"].exists():
        stats["list_page_failures"] = int(len(pd.read_csv(paths["list_pages_failed"])))
    else:
        stats["list_page_failures"] = 0
    return stats


def run_self_tests() -> None:
    """Minimal inline checks for parser helpers."""
    assert parse_iso_date("2025-02-08T11:00:00+00:00") == "2025-02-08"
    assert parse_mask("101X?") == [
        (1, "1", 1),
        (2, "0", 0),
        (3, "1", 1),
        (4, "X", 0),
        (5, "?", 0),
    ]
    tournament = {
        "id": 1,
        "name": "Test",
        "longName": "Test long",
        "dateStart": "2025-01-01T10:00:00+00:00",
        "dateEnd": "2025-01-01T12:00:00+00:00",
        "lastEditDate": "2025-01-02T10:00:00+00:00",
        "type": {"id": 2, "name": "Обычный"},
        "idseason": 60,
        "ratingSystems": ["mak", "chgkgg"],
        "editors": [{"id": 10}],
        "orgcommittee": [{"id": 20}],
    }
    result = {
        "team": {"id": 2, "name": "Team"},
        "current": {"name": "Team current"},
        "position": 1,
        "questionsTotal": 30,
        "mask": "110",
        "rating": {"inRating": True, "rg": 7000, "predictedPosition": 2},
        "teamMembers": [
            {
                "flag": "К",
                "rating": 100,
                "player": {
                    "id": 5,
                    "name": "Ivan",
                    "surname": "Ivanov",
                    "patronymic": "Ivanovich",
                },
            }
        ],
    }
    team_rows, player_rows, question_rows = flatten_tournament_results(
        tournament,
        [result],
        fetched_at="2025-01-03T00:00:00+00:00",
    )
    assert team_rows[0]["questions_total"] == 30
    assert team_rows[0]["teams_count"] == 1
    assert team_rows[0]["has_chgk_rating"] is True
    assert team_rows[0]["team_played_date"] == "2025-01-01"
    assert player_rows[0]["player_patronymic"] == "Ivanovich"
    assert question_rows[-1]["mask_char"] == "0"

    index = pd.DataFrame(
        [
            {
                "tournament_id": 1,
                "date_end": "2025-05-01",
                "last_edit_date": "2025-05-10",
            }
        ]
    )
    selected = select_tournament_ids_for_window(
        index,
        window_days=30,
        last_sync_at="2025-05-09T00:00:00+00:00",
    )
    assert selected == {1}


if __name__ == "__main__":
    run_self_tests()
    print("self tests passed")
