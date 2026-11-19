"""Turn one tournament payload into the four table row sets and c_star."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Optional

SYNCH_TYPE_IDS = {3, 6, 8}
MAIN_ROUND = 0

TOURNAMENT_COLUMNS = (
    "tournament_id",
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
    "question_qty",
    "teams_count",
    "rating_systems",
    "has_chgk_rating",
    "is_synch",
    "editors_ids",
    "orgcommittee_ids",
    "game_jury",
    "appeal_jury",
    "narrator",
    "inserted_at",
)

TEAM_COLUMNS = (
    "tournament_id",
    "team_id",
    "team_name",
    "current_team_name",
    "position",
    "predicted_position",
    "team_played_date",
    "venue_id",
    "venue_name",
    "town_name",
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
    "potential",
    "c_star",
    "player_ids",
    "flags",
    "inserted_at",
)

PLAYER_COLUMNS = (
    "tournament_id",
    "team_id",
    "player_id",
    "player_name",
    "player_surname",
    "player_patronymic",
    "flag",
    "player_rating",
    "inserted_at",
)

QUESTION_COLUMNS = (
    "tournament_id",
    "team_id",
    "tiebreak_round",
    "question_number",
    "mask_char",
    "taken",
    "difficulty",
    "question_potential",
    "inserted_at",
)

LOAD_COLUMNS = (
    "tournament_id",
    "last_edit_date",
    "status",
    "error",
    "inserted_at",
)


@dataclass
class Bundle:
    tournament: dict[str, Any]
    teams: list[dict[str, Any]]
    players: list[dict[str, Any]]
    questions: list[dict[str, Any]]
    hidden: bool
    active_questions: list[int] = field(default_factory=list)


def date_only(value: Any) -> Optional[str]:
    if value is None or value == "":
        return None
    text = str(value)
    if "T" in text:
        text = text.split("T", 1)[0]
    return text[:10]


def needs_requests(tournament: dict[str, Any], results: list[dict[str, Any]]) -> bool:
    type_id = (tournament.get("type") or {}).get("id")
    if type_id in SYNCH_TYPE_IDS:
        return True
    return any((row.get("synchRequest") or {}).get("id") for row in results)


def results_are_hidden(results: list[dict[str, Any]]) -> bool:
    if not results:
        return True
    return all(row.get("mask") is None and row.get("position") is None for row in results)


def _id_list(value: Any) -> list[int]:
    if not isinstance(value, list):
        return []
    ids: list[int] = []
    for item in value:
        if isinstance(item, dict) and item.get("id") is not None:
            ids.append(int(item["id"]))
    return ids


def _rating_systems_text(value: Any) -> str:
    if not value:
        return ""
    if isinstance(value, str):
        return value
    return ",".join(str(item) for item in value)


def _has_chgk_rating(value: Any) -> bool:
    if isinstance(value, str):
        systems = [part.strip() for part in value.split(",") if part.strip()]
    else:
        systems = list(value or [])
    return "chgkgg" in systems


def _canceled_numbers(tournament: dict[str, Any]) -> set[int]:
    raw = tournament.get("canceledQuestions") or []
    numbers: set[int] = set()
    if not isinstance(raw, list):
        return numbers
    for item in raw:
        if isinstance(item, int):
            numbers.add(item)
        elif isinstance(item, dict) and item.get("question") is not None:
            numbers.add(int(item["question"]))
    return numbers


def _request_index(requests: list[dict[str, Any]] | None) -> dict[int, dict[str, Any]]:
    index: dict[int, dict[str, Any]] = {}
    for row in requests or []:
        if isinstance(row, dict) and row.get("id") is not None:
            index[int(row["id"])] = row
    return index


def _played(request: dict[str, Any] | None, tournament: dict[str, Any], is_synch: bool) -> tuple[Any, Any, Any, Optional[str]]:
    if is_synch and request:
        venue = request.get("venue") if isinstance(request.get("venue"), dict) else {}
        town = venue.get("town") if isinstance(venue.get("town"), dict) else {}
        return venue.get("id"), venue.get("name"), town.get("name"), date_only(request.get("dateStart"))
    town = tournament.get("town") if isinstance(tournament.get("town"), dict) else {}
    return None, None, town.get("name"), date_only(tournament.get("dateStart"))


def _tiebreak_parts(mask: str | None) -> list[str]:
    if not mask:
        return []
    return [part for part in str(mask).split(" ") if part]


def compute_metrics(questions: list[dict[str, Any]], active: list[int]) -> dict[int, dict[str, Any]]:
    """potential is the mean difficulty of taken active questions.

    c_star uses the sum (potential * k): 0 means the easiest k, 1 the hardest k.
    Shootout rows and removed questions stay out of both numbers.
    """
    by_team: dict[int, dict[int, int]] = {}
    difficulty: dict[int, float] = {}
    for row in questions:
        if row["tiebreak_round"] != MAIN_ROUND:
            continue
        number = row["question_number"]
        if number not in active:
            continue
        by_team.setdefault(row["team_id"], {})[number] = row["taken"]
    if not active or not by_team:
        return {team_id: {"potential": None, "c_star": None} for team_id in by_team}

    for number in active:
        taken_values = [team_rows[number] for team_rows in by_team.values() if number in team_rows]
        difficulty[number] = sum(taken_values) / len(taken_values)

    ordered = [difficulty[number] for number in active]
    ease = sorted(ordered, reverse=True)
    prefix = [0.0]
    for value in ease:
        prefix.append(prefix[-1] + value)
    total = prefix[-1]
    count = len(ease)
    metrics: dict[int, dict[str, Any]] = {}
    for team_id, team_rows in by_team.items():
        taken_diffs = [difficulty[number] for number in active if team_rows.get(number) == 1]
        taken_count = len(taken_diffs)
        if taken_count == 0:
            metrics[team_id] = {"potential": None, "c_star": None}
            continue
        taken_sum = sum(taken_diffs)
        potential = taken_sum / taken_count
        if taken_count == count:
            c_star = None
        else:
            easiest = prefix[taken_count]
            hardest = total - prefix[count - taken_count]
            span = easiest - hardest
            c_star = (easiest - taken_sum) / span if span > 0 else None
            if c_star is not None and -1e-9 < c_star < 0:
                c_star = 0.0
            elif c_star is not None and 1 < c_star < 1 + 1e-9:
                c_star = 1.0
        metrics[team_id] = {"potential": potential, "c_star": c_star}
    return metrics


def _active_questions(questions: list[dict[str, Any]], canceled: set[int]) -> list[int]:
    chars: dict[int, list[str]] = {}
    for row in questions:
        if row["tiebreak_round"] != MAIN_ROUND:
            continue
        chars.setdefault(row["question_number"], []).append(row["mask_char"])
    active: list[int] = []
    for number in sorted(chars):
        if number in canceled:
            continue
        column = chars[number]
        if any(char == "X" for char in column):
            continue
        if not column or any(char not in {"0", "1"} for char in column):
            continue
        active.append(number)
    return active


def _apply_difficulty(questions: list[dict[str, Any]], active: list[int]) -> None:
    metrics_rows = [row for row in questions if row["tiebreak_round"] == MAIN_ROUND and row["question_number"] in active]
    by_number: dict[int, list[dict[str, Any]]] = {}
    for row in metrics_rows:
        by_number.setdefault(row["question_number"], []).append(row)
    for rows in by_number.values():
        difficulty = sum(row["taken"] for row in rows) / len(rows)
        for row in rows:
            row["difficulty"] = difficulty
            row["question_potential"] = difficulty * row["taken"]


def build_bundle(
    tournament: dict[str, Any],
    results: list[dict[str, Any]],
    requests: list[dict[str, Any]] | None,
    *,
    inserted_at: str,
) -> Bundle:
    is_synch = needs_requests(tournament, results)
    hidden = results_are_hidden(results)
    request_by_id = _request_index(requests)
    tournament_type = tournament.get("type") or {}
    rating_systems = tournament.get("ratingSystems")
    teams_count = tournament.get("teamsCount")
    if not isinstance(teams_count, int):
        teams_count = len(results)
    narrator = tournament.get("narrator") if isinstance(tournament.get("narrator"), dict) else None
    question_qty = tournament.get("questionQty") if isinstance(tournament.get("questionQty"), dict) else None
    tournament_row = {
        "tournament_id": int(tournament["id"]),
        "tournament_name": tournament.get("name"),
        "tournament_long_name": tournament.get("longName"),
        "date_start": date_only(tournament.get("dateStart")),
        "date_end": date_only(tournament.get("dateEnd")),
        "last_edit_date": date_only(tournament.get("lastEditDate")),
        "type_id": tournament_type.get("id"),
        "type_name": tournament_type.get("name"),
        "season_id": tournament.get("idseason"),
        "difficulty_forecast": tournament.get("difficultyForecast"),
        "true_dl": tournament.get("trueDL"),
        "question_qty": question_qty,
        "teams_count": teams_count,
        "rating_systems": _rating_systems_text(rating_systems),
        "has_chgk_rating": _has_chgk_rating(rating_systems),
        "is_synch": is_synch,
        "editors_ids": _id_list(tournament.get("editors")),
        "orgcommittee_ids": _id_list(tournament.get("orgcommittee")),
        "game_jury": _id_list(tournament.get("gameJury")),
        "appeal_jury": _id_list(tournament.get("appealJury")),
        "narrator": narrator,
        "inserted_at": inserted_at,
    }

    team_rows: list[dict[str, Any]] = []
    player_rows: list[dict[str, Any]] = []
    question_rows: list[dict[str, Any]] = []
    tournament_id = tournament_row["tournament_id"]

    for result in results:
        team = result.get("team") or {}
        current = result.get("current") or {}
        rating = result.get("rating") or {}
        if not isinstance(rating, dict):
            rating = {}
        team_id = int(team["id"])
        request = None
        if is_synch:
            request = request_by_id.get(int((result.get("synchRequest") or {})["id"]))
        venue_id, venue_name, town_name, played = _played(request, tournament, is_synch)
        members = result.get("teamMembers") or []
        player_ids: list[int] = []
        for member in members:
            player = member.get("player") or {}
            if player.get("id") is None:
                continue
            player_id = int(player["id"])
            player_ids.append(player_id)
            player_rows.append(
                {
                    "tournament_id": tournament_id,
                    "team_id": team_id,
                    "player_id": player_id,
                    "player_name": player.get("name"),
                    "player_surname": player.get("surname"),
                    "player_patronymic": player.get("patronymic"),
                    "flag": member.get("flag"),
                    "player_rating": member.get("rating"),
                    "inserted_at": inserted_at,
                }
            )
        flags = result.get("flags")
        team_rows.append(
            {
                "tournament_id": tournament_id,
                "team_id": team_id,
                "team_name": team.get("name"),
                "current_team_name": current.get("name"),
                "position": result.get("position"),
                "predicted_position": rating.get("predictedPosition"),
                "team_played_date": played,
                "venue_id": venue_id,
                "venue_name": venue_name,
                "town_name": town_name,
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
                "potential": None,
                "c_star": None,
                "player_ids": player_ids,
                "flags": flags if isinstance(flags, list) else [],
                "inserted_at": inserted_at,
            }
        )
        mask = result.get("mask")
        if isinstance(mask, str):
            for index, char in enumerate(mask, start=1):
                question_rows.append(
                    {
                        "tournament_id": tournament_id,
                        "team_id": team_id,
                        "tiebreak_round": MAIN_ROUND,
                        "question_number": index,
                        "mask_char": char,
                        "taken": 1 if char == "1" else 0,
                        "difficulty": None,
                        "question_potential": None,
                        "inserted_at": inserted_at,
                    }
                )
        for round_number, part in enumerate(_tiebreak_parts(result.get("tiebreaker")), start=1):
            for index, char in enumerate(part, start=1):
                question_rows.append(
                    {
                        "tournament_id": tournament_id,
                        "team_id": team_id,
                        "tiebreak_round": round_number,
                        "question_number": index,
                        "mask_char": char,
                        "taken": 1 if char == "1" else 0,
                        "difficulty": None,
                        "question_potential": None,
                        "inserted_at": inserted_at,
                    }
                )

    active = _active_questions(question_rows, _canceled_numbers(tournament))
    if not hidden:
        _apply_difficulty(question_rows, active)
        metrics = compute_metrics(question_rows, active)
        for team_row in team_rows:
            team_metrics = metrics.get(team_row["team_id"], {"potential": None, "c_star": None})
            team_row["potential"] = team_metrics["potential"]
            team_row["c_star"] = team_metrics["c_star"]
    return Bundle(
        tournament=tournament_row,
        teams=team_rows,
        players=player_rows,
        questions=question_rows,
        hidden=hidden,
        active_questions=active,
    )


def same_number(left: Any, right: Any) -> bool:
    if left is None or right is None:
        return left is None and right is None
    if isinstance(left, float) and math.isnan(left):
        return isinstance(right, float) and math.isnan(right)
    return math.isclose(float(left), float(right), rel_tol=0, abs_tol=1e-9)
