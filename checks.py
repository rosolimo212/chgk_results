"""Checks that run before any SQL write.

Response checks stop the write only when the tournament id is missing, the results payload is not a list, or two rows share a team id. A missing request, roster, mask, or flag becomes NULL and the tournament is still stored.
Bundle checks look at the rows that would be written.
"""

from __future__ import annotations

from typing import Any

from build import MAIN_ROUND, Bundle, compute_metrics, same_number

MAX_ERRORS = 20


def _add(errors: list[str], message: str) -> bool:
    if len(errors) < MAX_ERRORS:
        errors.append(message)
    return len(errors) >= MAX_ERRORS


def check_response(
    tournament: Any,
    results: Any,
    requests: list[dict[str, Any]] | None,
) -> list[str]:
    errors: list[str] = []
    if not isinstance(tournament, dict) or tournament.get("id") is None:
        _add(errors, "tournament payload has no id")
        return errors
    if not isinstance(results, list):
        _add(errors, "results payload is not a list")
        return errors

    seen_teams: set[int] = set()
    for row in results:
        if not isinstance(row, dict):
            continue
        team = row.get("team") if isinstance(row.get("team"), dict) else {}
        team_id = team.get("id")
        if team_id is None:
            continue
        if int(team_id) in seen_teams:
            if _add(errors, f"duplicate team.id {team_id}"):
                return errors
        seen_teams.add(int(team_id))
    return errors


def check_bundle(bundle: Bundle) -> list[str]:
    errors: list[str] = []
    tournament_id = bundle.tournament["tournament_id"]
    team_ids = [row["team_id"] for row in bundle.teams]
    if len(team_ids) != len(set(team_ids)):
        _add(errors, "team rows are not unique")
    team_id_set = set(team_ids)

    player_keys = [(row["team_id"], row["player_id"]) for row in bundle.players]
    if len(player_keys) != len(set(player_keys)):
        _add(errors, "player rows are not unique")
    question_keys = [
        (row["team_id"], row["tiebreak_round"], row["question_number"])
        for row in bundle.questions
    ]
    if len(question_keys) != len(set(question_keys)):
        _add(errors, "question rows are not unique")

    players_by_team: dict[int, list[int]] = {}
    for row in bundle.players:
        if row["tournament_id"] != tournament_id or row["team_id"] not in team_id_set:
            if _add(errors, f"player {row['player_id']} has no team row"):
                return errors
        players_by_team.setdefault(row["team_id"], []).append(row["player_id"])
        if row["inserted_at"] != bundle.tournament["inserted_at"]:
            _add(errors, "player inserted_at differs from the tournament")

    for row in bundle.questions:
        if row["tournament_id"] != tournament_id or row["team_id"] not in team_id_set:
            if _add(errors, f"question {row['question_number']} has no team row"):
                return errors
        if row["taken"] not in (0, 1, None):
            _add(errors, f"team {row['team_id']} question {row['question_number']} taken is not 0 or 1")
        if row["tiebreak_round"] < 0 or row["question_number"] < 1:
            _add(errors, "question key is out of range")
        if row["tiebreak_round"] == MAIN_ROUND and row["question_number"] in bundle.active_questions:
            if row["difficulty"] is None:
                _add(errors, f"active question {row['question_number']} has no difficulty")
            elif not same_number(row["question_potential"], row["difficulty"] * row["taken"]):
                _add(errors, f"question_potential does not match difficulty for question {row['question_number']}")
        elif row["difficulty"] is not None or row["question_potential"] is not None:
            _add(errors, f"removed or shootout question {row['question_number']} has difficulty")

    expected = compute_metrics(bundle.questions, bundle.active_questions)
    for row in bundle.teams:
        if row["tournament_id"] != tournament_id:
            _add(errors, "team tournament_id does not match")
        roster = players_by_team.get(row["team_id"], [])
        if row["player_ids"] is None:
            if roster:
                _add(errors, f"player_ids do not match the roster of team {row['team_id']}")
        elif row["player_ids"] != roster:
            _add(errors, f"player_ids do not match the roster of team {row['team_id']}")
        metrics = expected.get(row["team_id"], {"potential": None, "c_star": None})
        if not same_number(row["potential"], metrics["potential"]):
            _add(errors, f"potential does not match the taken questions of team {row['team_id']}")
        if not same_number(row["c_star"], metrics["c_star"]):
            _add(errors, f"c_star does not match the formula for team {row['team_id']}")
        c_star = row["c_star"]
        if c_star is not None and not 0 <= float(c_star) <= 1:
            _add(errors, f"c_star for team {row['team_id']} is outside 0..1")
    return errors
