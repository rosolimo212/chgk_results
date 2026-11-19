"""Checks that run before any SQL write.

Response checks look at the API payload as soon as it arrives.
Bundle checks look at the rows that would be written.
"""

from __future__ import annotations

from typing import Any

from build import MAIN_ROUND, Bundle, compute_metrics, needs_requests, same_number

MAX_ERRORS = 20
MASK_CHARS = set("01X?")
TIEBREAK_CHARS = set("01")


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

    request_ids = {
        int(row["id"])
        for row in (requests or [])
        if isinstance(row, dict) and row.get("id") is not None
    }
    synch = needs_requests(tournament, results)
    if synch and requests is None:
        _add(errors, "synch tournament has no requests payload")

    masks: list[str] = []
    seen_teams: set[int] = set()
    for index, row in enumerate(results):
        if not isinstance(row, dict):
            if _add(errors, f"result {index} is not an object"):
                return errors
            continue
        team = row.get("team") or {}
        team_id = team.get("id") if isinstance(team, dict) else None
        if team_id is None:
            if _add(errors, f"result {index} has no team.id"):
                return errors
            continue
        if int(team_id) in seen_teams:
            if _add(errors, f"duplicate team.id {team_id}"):
                return errors
        seen_teams.add(int(team_id))
        if "flags" not in row:
            if _add(errors, f"result {index} has no flags; includeTeamFlags was not applied"):
                return errors
        if not isinstance(row.get("flags"), list):
            if _add(errors, f"result {index} flags is not a list"):
                return errors
        mask = row.get("mask")
        if mask is not None:
            if not isinstance(mask, str) or any(char not in MASK_CHARS for char in mask):
                if _add(errors, f"result {index} mask has an unknown character"):
                    return errors
            else:
                masks.append(mask)
        tiebreaker = row.get("tiebreaker")
        if tiebreaker is not None:
            if not isinstance(tiebreaker, str):
                if _add(errors, f"result {index} tiebreaker is not a string"):
                    return errors
            else:
                parts = tiebreaker.split(" ")
                if any(part == "" or any(char not in TIEBREAK_CHARS for char in part) for part in parts):
                    if _add(errors, f"result {index} tiebreaker is not rounds of 0 and 1"):
                        return errors
        if synch:
            request_id = (row.get("synchRequest") or {}).get("id")
            if request_id is None or int(request_id) not in request_ids:
                if _add(errors, f"team {team_id} synchRequest is not in the requests list"):
                    return errors
            else:
                matched = next(
                    item
                    for item in requests or []
                    if isinstance(item, dict) and item.get("id") is not None and int(item["id"]) == int(request_id)
                )
                if not matched.get("dateStart"):
                    if _add(errors, f"team {team_id} request has no dateStart"):
                        return errors
    if masks and len(set(map(len, masks))) != 1:
        _add(errors, "team masks have different lengths")
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
        if row["taken"] not in (0, 1):
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
        if row["player_ids"] != players_by_team.get(row["team_id"], []):
            _add(errors, f"player_ids do not match the roster of team {row['team_id']}")
        metrics = expected.get(row["team_id"], {"potential": None, "c_star": None})
        if not same_number(row["potential"], metrics["potential"]):
            _add(errors, f"potential does not match the taken questions of team {row['team_id']}")
        if not same_number(row["c_star"], metrics["c_star"]):
            _add(errors, f"c_star does not match the formula for team {row['team_id']}")
        c_star = row["c_star"]
        if c_star is not None and not 0 <= float(c_star) <= 1:
            _add(errors, f"c_star for team {row['team_id']} is outside 0..1")
        if bundle.tournament["is_synch"] and not row["team_played_date"]:
            _add(errors, f"team {row['team_id']} has no played date")
    return errors
