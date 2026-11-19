import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import chgk_results as cr


FIXTURES = Path(__file__).parent / "fixtures"


class ParserTests(unittest.TestCase):
    def test_parse_mask(self):
        self.assertEqual(
            cr.parse_mask("101X?"),
            [(1, "1", 1), (2, "0", 0), (3, "1", 1), (4, "X", 0), (5, "?", 0)],
        )

    def test_has_chgk_rating(self):
        self.assertTrue(cr._has_chgk_rating(["mak", "chgkgg"]))
        self.assertFalse(cr._has_chgk_rating(["mak"]))

    def test_flatten_tournament_5022(self):
        tournament = json.loads((FIXTURES / "tournament_5022_meta.json").read_text())
        results = json.loads((FIXTURES / "tournament_5022_results.json").read_text())

        team_rows, player_rows, question_rows = cr.flatten_tournament_results(
            tournament,
            results,
            fetched_at="2025-01-01T00:00:00+00:00",
        )

        self.assertEqual(len(team_rows), 48)
        self.assertEqual(team_rows[0]["teams_count"], 48)
        self.assertEqual(team_rows[0]["questions_total"], 65)
        self.assertEqual(team_rows[0]["team_played_date"], "2018-06-02")
        self.assertFalse(team_rows[0]["has_chgk_rating"])
        self.assertNotIn("question_qty", team_rows[0])

        self.assertGreater(len(player_rows), 0)
        self.assertIn("player_patronymic", player_rows[0])

        self.assertGreater(len(question_rows), 0)
        self.assertEqual(
            set(question_rows[0].keys()),
            {
                "tournament_id",
                "team_id",
                "question_number",
                "mask_char",
                "taken",
                "fetched_at",
            },
        )

    def test_select_window(self):
        index = pd.DataFrame(
            [
                {"tournament_id": 1, "date_end": "2025-05-01", "last_edit_date": "2025-05-10"},
                {"tournament_id": 2, "date_end": "2020-01-01", "last_edit_date": "2020-01-01"},
            ]
        )
        selected = cr.select_tournament_ids_for_window(
            index,
            window_days=30,
            last_sync_at="2025-05-09T00:00:00+00:00",
        )
        self.assertEqual(selected, {1})

    def test_enrich_metrics(self):
        question_df = pd.DataFrame(
            [
                {"tournament_id": 1, "team_id": 10, "question_number": 1, "mask_char": "1", "taken": 1, "fetched_at": "t"},
                {"tournament_id": 1, "team_id": 10, "question_number": 2, "mask_char": "0", "taken": 0, "fetched_at": "t"},
                {"tournament_id": 1, "team_id": 20, "question_number": 1, "mask_char": "0", "taken": 0, "fetched_at": "t"},
                {"tournament_id": 1, "team_id": 20, "question_number": 2, "mask_char": "1", "taken": 1, "fetched_at": "t"},
            ]
        )
        team_df = pd.DataFrame([{"tournament_id": 1, "team_id": 10, "rg": 8000, "questions_total": 1}])
        team_df = pd.concat(
            [
                team_df,
                pd.DataFrame([{"tournament_id": 1, "team_id": 20, "rg": 6000, "questions_total": 1}]),
            ],
            ignore_index=True,
        )
        for column in cr.TEAM_PERFORMANCE_COLUMNS:
            if column not in team_df.columns:
                team_df[column] = pd.NA

        question_enriched = cr.enrich_question_results(question_df, team_df)
        self.assertEqual(
            question_enriched.loc[question_enriched["question_number"] == 1, "difficulty"].iloc[0],
            0.5,
        )
        probability = question_enriched.loc[
            (question_enriched["team_id"] == 10) & (question_enriched["question_number"] == 1),
            "take_probability",
        ].iloc[0]
        self.assertAlmostEqual(probability, 8000 / 7000 * 0.5)

        team_enriched = cr.enrich_team_performances(team_df, question_enriched)
        self.assertAlmostEqual(
            team_enriched.loc[team_enriched["team_id"] == 10, "total_potential"].iloc[0],
            0.5,
        )
        self.assertAlmostEqual(
            team_enriched.loc[team_enriched["team_id"] == 10, "potential"].iloc[0],
            0.5,
        )


class StorageTests(unittest.TestCase):
    def test_download_tournament_writes_shards(self):
        tournament = json.loads((FIXTURES / "tournament_5022_meta.json").read_text())
        results = json.loads((FIXTURES / "tournament_5022_results.json").read_text())

        with tempfile.TemporaryDirectory() as tmp_dir:
            paths = cr._ensure_layout(tmp_dir)

            with patch.object(cr, "fetch_tournament_bundle", return_value=(tournament, results, None)):
                status = cr.download_tournament(5022, paths, resume=False)

            self.assertEqual(status, "ok")
            self.assertTrue((paths["team_shards"] / "5022.parquet").exists())
            self.assertTrue((paths["player_shards"] / "5022.parquet").exists())
            self.assertTrue((paths["question_shards"] / "5022.parquet").exists())

            team_df = pd.read_parquet(paths["team_shards"] / "5022.parquet")
            self.assertEqual(len(team_df), 48)
            self.assertIn("questions_total", team_df.columns)
            self.assertIn("teams_count", team_df.columns)

            team_df, player_df, question_df = cr.compact_tables(tmp_dir)
            self.assertEqual(len(team_df), 48)
            self.assertGreater(len(player_df), 0)
            self.assertGreater(len(question_df), 0)
            self.assertTrue((paths["tables"] / "team_performances.parquet").exists())


if __name__ == "__main__":
    unittest.main()
