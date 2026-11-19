"""Commands agree, checks run before any write, and c_star matches the notebook."""

from __future__ import annotations

import json
import re
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from build import build_bundle
from checks import check_bundle, check_response
from commands import run_load_all, run_load_days, run_load_one, run_update
from config_load import load_settings
from chgk_results import main
from store import MemoryStore, statement_list

FIXTURES = Path(__file__).parent / "fixtures"
NOW = "2026-01-01T00:00:00+00:00"
SCHEMA = (ROOT / "schema.sql").read_text(encoding="utf-8")


def silence(*_args, **_kwargs) -> None:
    return None


def tournament_shell(**overrides):
    payload = {
        "id": 1,
        "name": "Турнир",
        "longName": "Турнир длинный",
        "dateStart": "2020-01-01T12:00:00+00:00",
        "dateEnd": "2020-01-01T18:00:00+00:00",
        "lastEditDate": "2020-01-02T00:00:00+00:00",
        "type": {"id": 2, "name": "Обычный"},
        "idseason": 1,
        "questionQty": {"1": 2},
        "ratingSystems": ["chgkgg"],
        "editors": [{"id": 5}],
        "orgcommittee": [],
        "gameJury": [],
        "appealJury": [],
        "trueDL": 1.5,
        "difficultyForecast": 2,
        "teamsCount": 3,
    }
    payload.update(overrides)
    return payload


def team_result(team_id: int, mask: str, tiebreaker: str | None = None, **overrides):
    row = {
        "team": {"id": team_id, "name": f"Команда {team_id}"},
        "current": {"name": f"Команда {team_id}"},
        "position": team_id,
        "questionsTotal": mask.count("1"),
        "rating": {"inRating": True, "rg": 1000, "r": 1, "rb": 1, "rt": 1, "b": 1, "d": 1, "d1": 0, "d2": 0, "bp": 1, "predictedPosition": team_id},
        "synchRequest": None,
        "mask": mask,
        "tiebreaker": tiebreaker,
        "flags": [{"id": 1, "shortName": "Б"}],
        "teamMembers": [
            {
                "flag": "Б",
                "rating": 100,
                "player": {"id": team_id * 10, "name": "Имя", "surname": "Фамилия", "patronymic": "Отчество"},
            }
        ],
    }
    row.update(overrides)
    return row


def three_team_payload():
    """q1 is taken twice, q2 once, so q1 is easier."""
    tournament = tournament_shell()
    results = [
        team_result(1, "11"),
        team_result(2, "10"),
        team_result(3, "01"),
    ]
    return tournament, results, None


class FakeClient:
    def __init__(self, tournament, results, requests=None, listed=None):
        self.tournament = tournament
        self.results = results
        self.requests = requests
        self.listed = listed if listed is not None else [tournament]
        self.fetched: list[int] = []
        self.list_calls: list[dict] = []

    def fetch(self, tournament_id: int):
        self.fetched.append(tournament_id)
        return self.tournament, self.results, self.requests

    def list_tournaments(self, params):
        self.list_calls.append(dict(params))
        return self.listed


class MetricTests(unittest.TestCase):
    def test_c_star_zero_is_easiest_and_one_is_hardest(self):
        # q1 taken by three teams, q2 by two, so q1 is easier.
        tournament = tournament_shell(teamsCount=4)
        results = [
            team_result(1, "11"),
            team_result(2, "10"),
            team_result(3, "10"),
            team_result(4, "01"),
        ]
        self.assertEqual(check_response(tournament, results, None), [])
        bundle = build_bundle(tournament, results, None, inserted_at=NOW)
        self.assertEqual(check_bundle(bundle), [])
        by_team = {row["team_id"]: row for row in bundle.teams}
        self.assertIsNone(by_team[1]["c_star"])
        self.assertAlmostEqual(by_team[1]["potential"], (0.75 + 0.5) / 2)
        self.assertAlmostEqual(by_team[2]["c_star"], 0.0)
        self.assertAlmostEqual(by_team[2]["potential"], 0.75)
        self.assertAlmostEqual(by_team[4]["c_star"], 1.0)
        self.assertAlmostEqual(by_team[4]["potential"], 0.5)

    def test_removed_and_shootout_questions_do_not_change_potential(self):
        tournament = tournament_shell(canceledQuestions=[2], questionQty={"1": 3})
        results = [
            team_result(1, "11X", tiebreaker="11"),
            team_result(2, "01X"),
        ]
        self.assertEqual(check_response(tournament, results, None), [])
        bundle = build_bundle(tournament, results, None, inserted_at=NOW)
        self.assertEqual(check_bundle(bundle), [])
        self.assertEqual(bundle.active_questions, [1])
        shootout = [row for row in bundle.questions if row["tiebreak_round"] == 1]
        self.assertEqual(len(shootout), 2)
        self.assertTrue(all(row["difficulty"] is None for row in shootout))
        removed = [row for row in bundle.questions if row["question_number"] == 2 and row["tiebreak_round"] == 0]
        self.assertTrue(all(row["difficulty"] is None for row in removed))
        by_team = {row["team_id"]: row for row in bundle.teams}
        self.assertAlmostEqual(by_team[1]["potential"], 0.5)
        self.assertIsNone(by_team[1]["c_star"])
        self.assertIsNone(by_team[2]["potential"])

    def test_corrupted_potential_is_rejected(self):
        tournament, results, requests = three_team_payload()
        bundle = build_bundle(tournament, results, requests, inserted_at=NOW)
        bundle.teams[1]["potential"] = 0.0
        errors = check_bundle(bundle)
        self.assertTrue(any("potential" in error for error in errors))


class ResponseCheckTests(unittest.TestCase):
    def test_missing_flags_and_bad_tiebreaker_and_unmatched_request(self):
        tournament, results, _requests = three_team_payload()
        broken = dict(results[0])
        del broken["flags"]
        self.assertTrue(check_response(tournament, [broken], None))

        bad_tie = team_result(1, "10", tiebreaker="10 2")
        self.assertTrue(any("tiebreaker" in error for error in check_response(tournament, [bad_tie], None)))

        synch = tournament_shell(type={"id": 3, "name": "Синхрон"})
        row = team_result(7, "10", synchRequest={"id": 99, "tournamentId": 1, "venue": {"id": 1, "name": "Зал"}})
        self.assertTrue(check_response(synch, [row], [{"id": 5, "dateStart": "2020-01-03T10:00:00+00:00"}]))
        ok_requests = [{"id": 99, "dateStart": "2020-01-03T10:00:00+00:00", "venue": {"id": 4, "name": "Зал", "town": {"name": "Рига"}}}]
        self.assertEqual(check_response(synch, [row], ok_requests), [])
        bundle = build_bundle(synch, [row], ok_requests, inserted_at=NOW)
        self.assertEqual(bundle.teams[0]["team_played_date"], "2020-01-03")
        self.assertEqual(bundle.teams[0]["town_name"], "Рига")
        self.assertTrue(bundle.tournament["is_synch"])


class CommandTests(unittest.TestCase):
    def _settings(self, write_sql: bool):
        example = json.loads((ROOT / "config.example.json").read_text(encoding="utf-8"))
        example["write_sql"] = write_sql
        example["workers"] = 1
        folder = tempfile.TemporaryDirectory()
        path = Path(folder.name) / "config.json"
        path.write_text(json.dumps(example), encoding="utf-8")
        if write_sql:
            (path.parent / "secrets.yaml").write_text(
                "database_url: postgresql://loader@127.0.0.1/chgk\n",
                encoding="utf-8",
            )
        settings = load_settings(path)
        self.addCleanup(folder.cleanup)
        return settings

    def test_three_commands_build_the_same_rows_and_do_not_write_when_disabled(self):
        tournament, results, requests = three_team_payload()
        client = FakeClient(tournament, results, requests)
        store = MemoryStore()
        settings = self._settings(False)
        one = run_load_one(client, store, 1, write_sql=settings.write_sql, now=NOW, echo=silence)
        update = run_update(client, store, write_sql=settings.write_sql, window_days=90, workers=1, now=NOW, echo=silence)
        days = run_load_days(client, store, days=730, write_sql=settings.write_sql, workers=1, now=NOW, echo=silence)
        full = run_load_all(client, store, write_sql=settings.write_sql, workers=1, now=NOW, echo=silence)
        self.assertEqual([item.action for item in one.outcomes], ["checked"])
        self.assertEqual([item.action for item in update.outcomes], ["checked"])
        self.assertEqual([item.action for item in days.outcomes], ["checked"])
        self.assertEqual([item.action for item in full.outcomes], ["checked"])
        self.assertEqual(one.outcomes[0].bundle.teams, update.outcomes[0].bundle.teams)
        self.assertEqual(one.outcomes[0].bundle.teams, days.outcomes[0].bundle.teams)
        self.assertEqual(one.outcomes[0].bundle.teams, full.outcomes[0].bundle.teams)
        self.assertEqual(one.outcomes[0].bundle.questions, update.outcomes[0].bundle.questions)
        self.assertEqual(one.outcomes[0].bundle.questions, days.outcomes[0].bundle.questions)
        self.assertEqual(one.outcomes[0].bundle.tournament, full.outcomes[0].bundle.tournament)
        self.assertEqual(store.writes, [])

    def test_write_flag_on_calls_replace_once_per_command(self):
        tournament, results, requests = three_team_payload()
        for runner in (
            lambda client, store: run_load_one(client, store, 1, write_sql=True, now=NOW, echo=silence),
            lambda client, store: run_update(client, store, write_sql=True, window_days=90, workers=1, now=NOW, echo=silence),
            lambda client, store: run_load_days(client, store, days=365, write_sql=True, workers=1, now=NOW, echo=silence),
            lambda client, store: run_load_all(client, store, write_sql=True, workers=1, now=NOW, echo=silence),
        ):
            client = FakeClient(tournament, results, requests)
            store = MemoryStore()
            report = runner(client, store)
            self.assertEqual(report.outcomes[0].action, "wrote")
            self.assertEqual(len(store.writes), 1)

    def test_hidden_results_do_not_replace_an_existing_mask(self):
        tournament = tournament_shell()
        hidden_row = team_result(1, "11")
        hidden_row["mask"] = None
        hidden_row["position"] = None
        hidden = [hidden_row]
        for starter in (
            lambda client, store: main(["load-one", "--id", "1"], client=client, store=store, settings=self._settings(True), now=NOW, echo=silence),
            lambda client, store: main(["update"], client=client, store=store, settings=self._settings(True), now=NOW, echo=silence),
            lambda client, store: main(["load-days", "--days", "365"], client=client, store=store, settings=self._settings(True), now=NOW, echo=silence),
            lambda client, store: main(["load-all"], client=client, store=store, settings=self._settings(True), now=NOW, echo=silence),
        ):
            client = FakeClient(tournament, hidden)
            store = MemoryStore()
            store.mask_ids.add(1)
            code = starter(client, store)
            self.assertEqual(code, 0)
            self.assertEqual(store.writes, [])

    def test_failed_check_blocks_write_for_load_one(self):
        tournament, results, requests = three_team_payload()
        del results[0]["flags"]
        client = FakeClient(tournament, results, requests)
        store = MemoryStore()
        report = run_load_one(client, store, 1, write_sql=True, now=NOW, echo=silence)
        self.assertEqual(report.outcomes[0].action, "failed")
        self.assertEqual(store.writes, [])
        self.assertTrue(report.failed)

    def test_full_reload_skips_an_unchanged_tournament(self):
        tournament, results, requests = three_team_payload()
        client = FakeClient(tournament, results, requests)
        store = MemoryStore()
        store.loaded[1] = ("2020-01-02", "ok")
        report = run_load_all(client, store, write_sql=True, workers=1, now=NOW, echo=silence)
        self.assertEqual(report.outcomes[0].action, "skipped_resume")
        self.assertEqual(client.fetched, [])
        self.assertEqual(store.writes, [])

    def test_update_also_asks_for_edits_after_last_success(self):
        tournament, results, requests = three_team_payload()
        client = FakeClient(tournament, results, requests)
        store = MemoryStore()
        store.success_at = "2026-01-01T00:00:00+00:00"
        run_update(client, store, write_sql=False, window_days=90, workers=1, now=NOW, echo=silence)
        self.assertTrue(any("dateEnd[after]" in call for call in client.list_calls))
        self.assertTrue(any("lastEditDate[after]" in call for call in client.list_calls))

    def test_cli_commands_share_the_pipeline(self):
        tournament, results, requests = three_team_payload()
        settings = self._settings(False)
        rows = []
        for argv in (["load-one", "--id", "1"], ["update"], ["load-days", "--days", "365"], ["load-all"]):
            client = FakeClient(tournament, results, requests)
            store = MemoryStore()
            code = main(argv, client=client, store=store, settings=settings, now=NOW, echo=silence)
            self.assertEqual(code, 0)
            self.assertEqual(store.writes, [])
            report_client = client
            rows.append(report_client.fetched)
        self.assertEqual(rows, [[1], [1], [1], [1]])

    def test_load_days_uses_only_the_requested_window_and_refetches(self):
        tournament, results, requests = three_team_payload()
        client = FakeClient(tournament, results, requests)
        store = MemoryStore()
        store.loaded[1] = ("2020-01-02", "ok")
        store.success_at = "2026-01-01T00:00:00+00:00"
        day = datetime(2026, 6, 15, tzinfo=timezone.utc)
        report = run_load_days(
            client,
            store,
            days=730,
            write_sql=False,
            workers=1,
            now=NOW,
            today=day,
            echo=silence,
        )
        cutoff = (day.date() - timedelta(days=730)).isoformat()
        self.assertEqual(client.list_calls, [{"dateEnd[after]": cutoff}])
        self.assertEqual(client.fetched, [1])
        self.assertEqual(report.outcomes[0].action, "checked")

    def test_fixture_5022_passes_checks(self):
        tournament = json.loads((FIXTURES / "tournament_5022_meta.json").read_text(encoding="utf-8"))
        results = json.loads((FIXTURES / "tournament_5022_results.json").read_text(encoding="utf-8"))
        for row in results:
            row["flags"] = []
        self.assertEqual(check_response(tournament, results, None), [])
        bundle = build_bundle(tournament, results, None, inserted_at=NOW)
        self.assertEqual(check_bundle(bundle), [])
        self.assertEqual(len(bundle.teams), 48)
        self.assertEqual(bundle.teams[0]["team_played_date"], "2018-06-02")
        self.assertFalse(bundle.tournament["is_synch"])
        self.assertFalse(bundle.tournament["has_chgk_rating"])
        self.assertEqual(bundle.teams[0]["player_ids"], [row["player_id"] for row in bundle.players if row["team_id"] == bundle.teams[0]["team_id"]])
        self.assertGreater(len(bundle.questions), 0)
        self.assertTrue(all(row["tiebreak_round"] == 0 for row in bundle.questions))


class SchemaTests(unittest.TestCase):
    def test_python_columns_are_declared(self):
        from build import LOAD_COLUMNS, PLAYER_COLUMNS, QUESTION_COLUMNS, TEAM_COLUMNS, TOURNAMENT_COLUMNS

        tables = {
            "chgk.tournaments (": TOURNAMENT_COLUMNS,
            "chgk.tournaments_with_teams (": TEAM_COLUMNS,
            "chgk.tournaments_with_players (": PLAYER_COLUMNS,
            "chgk.tournaments_with_questions (": QUESTION_COLUMNS,
            "chgk.tournament_load (": LOAD_COLUMNS,
        }
        for marker, columns in tables.items():
            start = SCHEMA.index(marker)
            block = SCHEMA[start:SCHEMA.index(");", start)]
            for column in columns:
                self.assertRegex(block, rf"(?m)^\s*{re.escape(column)}\s+")

    def test_sql_checks_run_before_delete(self):
        statements = statement_list()
        validate_at = next(index for index, sql in enumerate(statements) if "tmp_questions" in sql and sql.startswith("select"))
        delete_at = next(index for index, sql in enumerate(statements) if sql.startswith("delete from"))
        self.assertLess(validate_at, delete_at)
        self.assertIn("create temp table", statements[0])

    def test_example_config_disables_sql_and_requires_a_url_to_enable_it(self):
        settings = load_settings(ROOT / "config.example.json")
        self.assertFalse(settings.write_sql)
        self.assertEqual(settings.window_days, 90)
        example = json.loads((ROOT / "config.example.json").read_text(encoding="utf-8"))
        example["write_sql"] = True
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(json.dumps(example), encoding="utf-8")
            with self.assertRaises(ValueError):
                load_settings(path)


if __name__ == "__main__":
    unittest.main()
