"""Postgres replacement of one tournament, and an in-memory stand-in for tests.

New rows land in temporary tables. Counts are checked there. Only then are the
old rows of this tournament deleted and the new ones inserted, in one transaction.
"""

from __future__ import annotations

import threading
from typing import Any

from build import (
    LOAD_COLUMNS,
    PLAYER_COLUMNS,
    QUESTION_COLUMNS,
    TEAM_COLUMNS,
    TOURNAMENT_COLUMNS,
    Bundle,
)

# Executed in this order. Validation stays ahead of the delete.
REPLACEMENT_SQL = (
    "create temp table tmp_tournaments on commit drop as select * from chgk.tournaments where false",
    "create temp table tmp_teams on commit drop as select * from chgk.tournaments_with_teams where false",
    "create temp table tmp_players on commit drop as select * from chgk.tournaments_with_players where false",
    "create temp table tmp_questions on commit drop as select * from chgk.tournaments_with_questions where false",
    "insert into tmp_tournaments ({tournament_columns}) values ({tournament_placeholders})",
    "insert into tmp_teams ({team_columns}) values ({team_placeholders})",
    "insert into tmp_players ({player_columns}) values ({player_placeholders})",
    "insert into tmp_questions ({question_columns}) values ({question_placeholders})",
    """
    select count(*) from tmp_questions q
    where not exists (
        select 1 from tmp_teams t
        where t.tournament_id = q.tournament_id and t.team_id = q.team_id
    )
    """.strip(),
    """
    select count(*) from tmp_teams
    where c_star is not null and (c_star < 0 or c_star > 1)
    """.strip(),
    "delete from chgk.tournament_load where tournament_id = %(tournament_id)s",
    "delete from chgk.tournaments_with_questions where tournament_id = %(tournament_id)s",
    "delete from chgk.tournaments_with_players where tournament_id = %(tournament_id)s",
    "delete from chgk.tournaments_with_teams where tournament_id = %(tournament_id)s",
    "delete from chgk.tournaments where tournament_id = %(tournament_id)s",
    "insert into chgk.tournaments select * from tmp_tournaments",
    "insert into chgk.tournaments_with_teams select * from tmp_teams",
    "insert into chgk.tournaments_with_players select * from tmp_players",
    "insert into chgk.tournaments_with_questions select * from tmp_questions",
    "insert into chgk.tournament_load ({load_columns}) values ({load_placeholders})",
)


def _placeholders(columns: tuple[str, ...]) -> str:
    return ", ".join(f"%({name})s" for name in columns)


def statement_list() -> list[str]:
    rendered = []
    for statement in REPLACEMENT_SQL:
        rendered.append(
            statement.format(
                tournament_columns=", ".join(TOURNAMENT_COLUMNS),
                tournament_placeholders=_placeholders(TOURNAMENT_COLUMNS),
                team_columns=", ".join(TEAM_COLUMNS),
                team_placeholders=_placeholders(TEAM_COLUMNS),
                player_columns=", ".join(PLAYER_COLUMNS),
                player_placeholders=_placeholders(PLAYER_COLUMNS),
                question_columns=", ".join(QUESTION_COLUMNS),
                question_placeholders=_placeholders(QUESTION_COLUMNS),
                load_columns=", ".join(LOAD_COLUMNS),
                load_placeholders=_placeholders(LOAD_COLUMNS),
            )
        )
    return rendered


def _json_value(value: Any) -> Any:
    from psycopg.types.json import Json

    if value is None:
        return None
    return Json(value)


def _bind(row: dict[str, Any], columns: tuple[str, ...], json_columns: set[str]) -> dict[str, Any]:
    bound = {}
    for name in columns:
        value = row[name]
        bound[name] = _json_value(value) if name in json_columns else value
    return bound


class MemoryStore:
    """Records what would be written. Used when SQL writing is off and in tests."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.mask_ids: set[int] = set()
        self.loaded: dict[int, tuple[str | None, str]] = {}
        self.success_at: str | None = None
        self.writes: list[Bundle] = []

    def has_published_mask(self, tournament_id: int) -> bool:
        with self._lock:
            return tournament_id in self.mask_ids

    def loaded_edit(self, tournament_id: int) -> str | None:
        with self._lock:
            row = self.loaded.get(tournament_id)
        if row and row[1] == "ok":
            return row[0]
        return None

    def latest_success_at(self) -> str | None:
        with self._lock:
            return self.success_at

    def replace_tournament(self, bundle: Bundle) -> None:
        with self._lock:
            self.writes.append(bundle)
            tournament_id = bundle.tournament["tournament_id"]
            self.loaded[tournament_id] = (bundle.tournament["last_edit_date"], "ok")
            if any(row["tiebreak_round"] == 0 and row["mask_char"] is not None for row in bundle.questions):
                self.mask_ids.add(tournament_id)
            self.success_at = bundle.tournament["inserted_at"]


class PostgresStore:
    def __init__(self, database_url: str, *, allow_write: bool) -> None:
        self._database_url = database_url
        self._allow_write = allow_write

    def _connect(self):
        import psycopg

        return psycopg.connect(self._database_url, connect_timeout=15)

    def has_published_mask(self, tournament_id: int) -> bool:
        query = """
            select exists (
                select 1
                from chgk.tournaments_with_questions
                where tournament_id = %s
                  and tiebreak_round = 0
                  and mask_char is not null
            )
        """
        with self._connect() as conn:
            row = conn.execute(query, (tournament_id,)).fetchone()
        return bool(row and row[0])

    def loaded_edit(self, tournament_id: int) -> str | None:
        query = """
            select to_char(last_edit_date, 'YYYY-MM-DD')
            from chgk.tournament_load
            where tournament_id = %s and status = 'ok'
        """
        with self._connect() as conn:
            row = conn.execute(query, (tournament_id,)).fetchone()
        return row[0] if row else None

    def latest_success_at(self) -> str | None:
        query = """
            select to_char(max(inserted_at) at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"+00:00"')
            from chgk.tournament_load
            where status = 'ok'
        """
        with self._connect() as conn:
            row = conn.execute(query).fetchone()
        return row[0] if row and row[0] else None

    def replace_tournament(self, bundle: Bundle) -> None:
        if not self._allow_write:
            raise RuntimeError("SQL writing is disabled")
        statements = statement_list()
        json_columns = {
            "question_qty",
            "editors_ids",
            "orgcommittee_ids",
            "game_jury",
            "appeal_jury",
            "narrator",
            "player_ids",
            "flags",
        }
        tournament_id = bundle.tournament["tournament_id"]
        load_row = {
            "tournament_id": tournament_id,
            "last_edit_date": bundle.tournament["last_edit_date"],
            "status": "ok",
            "error": None,
            "inserted_at": bundle.tournament["inserted_at"],
        }
        with self._connect() as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    for statement in statements[:4]:
                        cur.execute(statement)
                    cur.executemany(statements[4], [_bind(bundle.tournament, TOURNAMENT_COLUMNS, json_columns)])
                    if bundle.teams:
                        cur.executemany(statements[5], [_bind(row, TEAM_COLUMNS, json_columns) for row in bundle.teams])
                    if bundle.players:
                        cur.executemany(statements[6], [_bind(row, PLAYER_COLUMNS, json_columns) for row in bundle.players])
                    if bundle.questions:
                        cur.executemany(
                            statements[7],
                            [_bind(row, QUESTION_COLUMNS, json_columns) for row in bundle.questions],
                        )
                    cur.execute(statements[8])
                    orphan_count = cur.fetchone()[0]
                    cur.execute(statements[9])
                    star_count = cur.fetchone()[0]
                    if orphan_count or star_count:
                        raise RuntimeError("temporary rows failed the SQL check")
                    for statement in statements[10:15]:
                        cur.execute(statement, {"tournament_id": tournament_id})
                    for statement in statements[15:19]:
                        cur.execute(statement)
                    cur.execute(statements[19], _bind(load_row, LOAD_COLUMNS, set()))
