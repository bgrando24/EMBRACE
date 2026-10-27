import unittest

import pandas as pd

from ml import Freshness
from ml.outcomes import pick_outcomes, report_lines, summarise_outcomes
from test_freshness import ADDITIONS


def logged(day, t_const, lst="fresh", user="u1", rank=1):
    return {"generated_at": f"{day} 04:00:00", "user_id": user, "list": lst, "rank": rank, "t_const": t_const}


def watch(t_const, first, hours=2.0, user="u1"):
    return {"user_id": user, "imdb_id": t_const, "unit_type": "Movie", "total_seconds_watched": hours * 3600,
            "runtime_seconds": 7200, "first_watched_timestamp": f"{first} 20:00:00"}


NOW = "2026-04-01"
LOG = pd.DataFrame([
    logged("2026-03-01", "tt1"), logged("2026-03-02", "tt1"),       # one pick, shown two days
    logged("2026-03-01", "tt4", rank=2),
    logged("2026-03-01", "tt6", "back_catalogue"),
    logged("2026-03-01", "tt7", "back_catalogue", rank=2),
    logged("2026-03-30", "tt3", "back_catalogue", rank=3),          # window still open on NOW
    logged("2026-03-01", "tt-off", "to_request"),
])
HISTORY = pd.DataFrame([
    watch("tt1", "2026-03-10"),                 # within 14 days of the last day shown, all of it: liked
    watch("tt4", "2026-03-20", hours=0.5),      # 18 days after: too late
    watch("tt6", "2026-03-05", hours=0.5),      # in time, a quarter of it: started, not liked
    watch("tt3", "2026-03-31"),                 # in an open window
    watch("tt2", "2026-03-03"),                 # not a pick: counts for the baseline
])
REQUESTS = pd.DataFrame([{"user_id": "u1", "t_const": "tt-off", "requested_at": "2026-03-04"}])


class TestPickOutcomes(unittest.TestCase):

    def setUp(self):
        self.picks = pick_outcomes(LOG, HISTORY, REQUESTS, Freshness(ADDITIONS), now=NOW).set_index("t_const")

    def test_each_pick_once_with_its_window(self):
        self.assertEqual(len(self.picks), 6)
        self.assertEqual(self.picks.loc["tt1", "days_offered"], 2)
        p = self.picks
        self.assertEqual((p.loc["tt1", "watched"], p.loc["tt1", "liked"]), (True, True))
        self.assertFalse(p.loc["tt4", "watched"])                               # after the window
        self.assertEqual((p.loc["tt6", "watched"], p.loc["tt6", "liked"]), (True, False))
        self.assertTrue(p.loc["tt-off", "requested"])

    def test_open_windows_are_pending_even_if_watched(self):
        self.assertTrue(self.picks.loc["tt3", "pending"])
        self.assertTrue(self.picks.loc["tt3", "watched"])
        self.assertFalse(self.picks["pending"].drop("tt3").any())

    def test_baseline_is_the_chance_of_a_random_unwatched_title(self):
        # tt1's window (1-16 Mar): u1 started tt1, tt2 and tt6, of the 7 titles on the server they hadn't watched
        self.assertAlmostEqual(self.picks.loc["tt1", "expected"], 3 / 7)
        self.assertTrue(pd.isna(self.picks.loc["tt-off", "expected"]))         # not on the server

    def test_summary_rates_count_closed_windows_only(self):
        s = summarise_outcomes(self.picks.reset_index())
        self.assertEqual(s.loc["fresh", ["picks", "watched", "liked", "open"]].tolist(), [2, 1, 1, 0])
        self.assertEqual(s.loc["back_catalogue", ["picks", "watched", "open", "open_watched"]].tolist(), [2, 1, 1, 1])
        self.assertAlmostEqual(s.loc["back_catalogue", "watched_rate"], 0.5)
        lines = report_lines(s, {"fresh": "new", "to_request": "to request"})
        self.assertTrue(any(line.startswith("new: 1 of 2 watched (50%, ") for line in lines))
        self.assertIn("to request: 1 of 1 requested, 0 watched", lines)

    def test_nothing_logged(self):
        self.assertTrue(pick_outcomes(LOG.iloc[0:0], HISTORY, REQUESTS, now=NOW).empty)
        self.assertTrue(summarise_outcomes(pick_outcomes(LOG.iloc[0:0], HISTORY, REQUESTS, now=NOW)).empty)


if __name__ == "__main__":
    unittest.main()
