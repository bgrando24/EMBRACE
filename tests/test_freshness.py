import unittest

import pandas as pd

from ml import ContentKNN, Freshness
from ml.evaluate import freshness_ranker, next_watch, profile_ranker
from test_content_knn import CONTENT


def added(t_const, when, item_type="Movie"):
    return {"item_id": f"{t_const}-{when}", "item_type": item_type, "t_const": t_const, "added_at": pd.Timestamp(when, tz="UTC")}


ADDITIONS = pd.DataFrame([
    added("tt1", "2026-01-01"),
    added("tt2", "2026-03-01"),
    added("tt3", "2025-06-01"),
    added("tt4", "2026-02-01"),
    added("tt5", "2025-01-01", "Series"),
    added("tt5", "2026-02-25", "Episode"),     # a new episode makes the show fresh again
    added("tt6", "2025-01-01"),
    added("tt7", "2025-01-01"),
    {"item_id": "old", "item_type": "Movie", "t_const": "tt-old", "added_at": pd.NaT},    # removed before dates were kept
])


class TestFreshness(unittest.TestCase):

    def setUp(self):
        self.f = Freshness(ADDITIONS)

    def test_at_only_counts_items_added_by_then(self):
        at = self.f.at("2026-02-10")
        self.assertNotIn("tt2", at.index)          # not added yet
        self.assertNotIn("tt-old", at.index)       # no date
        self.assertEqual(at.loc["tt5", "last_added"], pd.Timestamp("2025-01-01"))
        self.assertEqual(at.loc["tt4", "age_days"], 9)

    def test_new_episode_refreshes_a_series(self):
        at = self.f.at("2026-03-01")
        self.assertEqual(at.loc["tt5", "first_added"], pd.Timestamp("2025-01-01"))
        self.assertEqual(at.loc["tt5", "age_days"], 4)

    def test_boost_halves_every_half_life(self):
        b = self.f.boost("2026-03-01", weight=0.4, half_life_days=28)
        self.assertAlmostEqual(b["tt2"], 0.4)
        self.assertAlmostEqual(b["tt4"], 0.2)      # 28 days old

    def test_boost_reorders_profile_recommendations(self):
        knn = ContentKNN(CONTENT)
        profile = knn.profile_vector(["tt3"], [1.0])
        plain = knn.recommend_for_profile(profile, n=2, exclude=["tt3"])
        self.assertEqual(plain.loc[0, "t_const"], "tt4")     # shares detective + person 2
        boosted = knn.recommend_for_profile(profile, n=2, exclude=["tt3"], boost=pd.Series({"tt2": 5.0, "tt-unknown": 9.0}))
        self.assertEqual(boosted["t_const"].tolist(), ["tt2", "tt4"])
        self.assertAlmostEqual(boosted.loc[0, "score"], boosted.loc[0, "similarity"] + 5.0, places=5)
        self.assertEqual(boosted.loc[1, "boost"], 0)


class TestNextWatch(unittest.TestCase):

    def history(self):
        def row(imdb_id, first_watched, hours=2):
            return {"user_id": "u1", "user_name": "u1", "imdb_id": imdb_id, "unit_name": imdb_id, "unit_type": "Movie",
                    "items_watched": 1, "total_seconds_watched": hours * 3600, "runtime_seconds": 7200,
                    "first_watched_timestamp": first_watched}
        return pd.DataFrame([
            row("tt3", "2025-07-01 20:00:00"),
            row("tt1", "2026-01-05 20:00:00"),
            row("tt4", "2026-02-02 20:00:00"),
            row("tt2", "2026-02-15 20:00:00"),     # watched before Emby says it was added (e.g. re-imported later)
        ])

    def setUp(self):
        self.f = Freshness(ADDITIONS)
        self.knn = ContentKNN(CONTENT)
        self.scope = {t for t in CONTENT["t_const"] if t in self.knn}

    def test_only_earlier_history_and_titles_on_the_server_then(self):
        res = next_watch(freshness_ranker(self.f), self.history(), self.scope, self.f).set_index("imdb_id")
        # tt3 is the first watch (no history), tt2 wasn't on the server yet
        self.assertEqual(sorted(res.index), ["tt1", "tt4"])
        # at 2026-01-05: tt1, tt3 (watched), tt5, tt6, tt7 are on the server -> tt1 is the newest of the 4 unwatched
        self.assertEqual(res.loc["tt1", ["rank", "n_ranked"]].tolist(), [1, 4])
        # at 2026-02-02: tt4 added the day before is newest; tt1 + tt3 are excluded as watched
        self.assertEqual(res.loc["tt4", ["rank", "n_ranked"]].tolist(), [1, 4])

    def test_profile_ranker_runs_time_aware(self):
        boosted = profile_ranker(self.knn, lambda when: self.f.boost(when))
        res = next_watch(boosted, self.history(), self.scope, self.f).set_index("imdb_id")
        self.assertEqual(res.loc["tt4", "rank"], 1)


if __name__ == "__main__":
    unittest.main()
