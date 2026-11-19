"""HTTP client for the public rating API."""

from __future__ import annotations

import threading
import time
from typing import Any

import requests

from build import SYNCH_TYPE_IDS
from config_load import Settings

RESULTS_PARAMS = {
    "includeTeamMembers": "1",
    "includeMasksAndControversials": "1",
    "includeRatingB": "1",
    "includeTeamFlags": "1",
}
RETRY_STATUSES = {429, 500, 502, 503, 504}


class ApiError(Exception):
    pass


class _Limiter:
    def __init__(self, delay_seconds: float) -> None:
        self.delay_seconds = delay_seconds
        self._lock = threading.Lock()
        self._next_at = 0.0

    def wait(self) -> None:
        if self.delay_seconds <= 0:
            return
        with self._lock:
            now = time.monotonic()
            if now < self._next_at:
                time.sleep(self._next_at - now)
            self._next_at = time.monotonic() + self.delay_seconds


class HttpClient:
    def __init__(self, settings: Settings) -> None:
        self._base = settings.api_base
        self._items_per_page = settings.items_per_page
        self._token = settings.api_token
        self._limiter = _Limiter(settings.request_delay_seconds)
        self._local = threading.local()

    def _headers(self) -> dict[str, str]:
        headers = {"accept": "application/json"}
        if self._token:
            headers["authorization"] = f"Bearer {self._token}"
        return headers

    def _session(self) -> requests.Session:
        session = getattr(self._local, "session", None)
        if session is None:
            session = requests.Session()
            self._local.session = session
        return session

    def _get_json(self, path: str, params: dict[str, Any] | None = None, timeout: int = 60) -> Any:
        url = f"{self._base}/{path.lstrip('/')}"
        last_error = f"{path} failed"
        for attempt in range(1, 6):
            self._limiter.wait()
            try:
                response = self._session().get(url, params=params, headers=self._headers(), timeout=timeout)
            except requests.RequestException as exc:
                last_error = f"{path} {exc.__class__.__name__}"
                time.sleep(min(2 ** attempt, 30))
                continue
            if response.status_code == 200:
                try:
                    return response.json()
                except ValueError as exc:
                    raise ApiError(f"{path} returned non-json") from exc
            last_error = f"{path} status {response.status_code}"
            if response.status_code in RETRY_STATUSES:
                time.sleep(min(2 ** attempt, 30))
                continue
            raise ApiError(last_error)
        raise ApiError(last_error)

    def fetch(self, tournament_id: int) -> tuple[Any, Any, list[dict[str, Any]] | None]:
        tournament = self._get_json(f"tournaments/{tournament_id}.json")
        results = self._get_json(
            f"tournaments/{tournament_id}/results.json",
            params=RESULTS_PARAMS,
        )
        requests_rows = None
        type_id = None
        if isinstance(tournament, dict):
            type_id = (tournament.get("type") or {}).get("id")
        if type_id in SYNCH_TYPE_IDS or _results_have_request(results):
            requests_rows = self._paged(f"tournaments/{tournament_id}/requests.json")
        return tournament, results, requests_rows

    def list_tournaments(self, params: dict[str, Any]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        page = 1
        while True:
            query = {"page": page, "itemsPerPage": self._items_per_page, **params}
            chunk = self._get_json("tournaments.json", params=query, timeout=120)
            if not isinstance(chunk, list):
                raise ApiError("tournament list is not a list")
            if not chunk:
                break
            rows.extend(chunk)
            if len(chunk) < self._items_per_page:
                break
            page += 1
        return rows

    def _paged(self, path: str) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        page = 1
        while True:
            chunk = self._get_json(
                path,
                params={"page": page, "itemsPerPage": self._items_per_page},
            )
            if not isinstance(chunk, list):
                raise ApiError(f"{path} is not a list")
            if not chunk:
                break
            rows.extend(chunk)
            if len(chunk) < self._items_per_page:
                break
            page += 1
        return rows


def _results_have_request(results: Any) -> bool:
    if not isinstance(results, list):
        return False
    return any(isinstance(row, dict) and (row.get("synchRequest") or {}).get("id") for row in results)
