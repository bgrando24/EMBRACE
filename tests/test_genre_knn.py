import unittest

import pandas as pd

from ml import GenreKNN, PreProcess


def rows(*titles):
    """titles: (t_const, name, type, num_votes, [genres])"""
    out = []
    for t_const, name, ttype, votes, genres in titles:
        for g in genres:
            out.append({"t_const": t_const, "primary_name": name, "type": ttype, "start_year": "2000",
                        "avg_rating": 7.0, "num_votes": votes, "genre": g})
    return pd.DataFrame(out)


TITLES = rows(
    ("tt1", "Seed Show", "tvSeries", 100, ["Comedy", "Crime"]),
    ("tt2", "Exact Match Popular", "movie", 900, ["Comedy", "Crime"]),
    ("tt3", "Exact Match Less Popular", "tvSeries", 50, ["Crime", "Comedy"]),
    ("tt4", "Exact Match Same Votes B", "movie", 50, ["Comedy", "Crime"]),
    ("tt5", "Partial Match", "movie", 99999, ["Comedy"]),
    ("tt6", "No Overlap", "movie", 10**6, ["Horror"]),
    ("tt7", "No Votes", "movie", None, ["Comedy", "Crime"]),
)


class TestEncodeTitleGenres(unittest.TestCase):

    def test_one_row_per_title_with_sorted_vocabulary(self):
        enc = PreProcess.encode_title_genres(TITLES)
        self.assertEqual(len(enc), 7)
        self.assertEqual([c for c in enc.columns if c.startswith("genre_")], ["genre_Comedy", "genre_Crime", "genre_Horror"])
        self.assertEqual(enc.set_index("t_const").loc["tt3", ["genre_Comedy", "genre_Crime", "genre_Horror"]].tolist(), [1, 1, 0])

    def test_duplicate_genre_rows_do_not_double_count(self):
        enc = PreProcess.encode_title_genres(pd.concat([TITLES, TITLES.iloc[:1]]))
        self.assertEqual(enc.set_index("t_const").loc["tt1", "genre_Comedy"], 1)


class TestGenreKNN(unittest.TestCase):

    def setUp(self):
        self.knn = GenreKNN(PreProcess.encode_title_genres(TITLES))

    def test_seed_is_never_recommended(self):
        self.assertNotIn("tt1", set(self.knn.recommend("tt1", n=10)["t_const"]))

    def test_ranking_and_tie_break(self):
        # exact matches first, ties by votes desc (missing votes = 0), then t_const; then partial; no-overlap last
        result = self.knn.recommend("tt1", n=6)
        self.assertEqual(result["t_const"].tolist(), ["tt2", "tt3", "tt4", "tt7", "tt5", "tt6"])
        self.assertAlmostEqual(result["similarity"].iloc[0], 1.0)
        self.assertAlmostEqual(result.set_index("t_const").loc["tt5", "similarity"], 0.7071, places=4)
        self.assertEqual(result.set_index("t_const").loc["tt5", "shared_genres"], ["Comedy"])

    def test_n_limits_results_and_cuts_ties_deterministically(self):
        self.assertEqual(self.knn.recommend("tt1", n=2)["t_const"].tolist(), ["tt2", "tt3"])
        self.assertTrue(self.knn.recommend("tt1", n=3).equals(self.knn.recommend("tt1", n=3)))

    def test_allowed_filter_applies_before_ranking(self):
        result = self.knn.recommend("tt1", n=5, allowed=["tt6", "tt5", "tt1"])
        self.assertEqual(result["t_const"].tolist(), ["tt5", "tt6"])

    def test_type_filter(self):
        self.assertEqual(self.knn.recommend("tt1", n=5, types=["tvSeries"])["t_const"].tolist(), ["tt3"])

    def test_empty_filter_result(self):
        self.assertTrue(self.knn.recommend("tt1", allowed=["tt999"]).empty)

    def test_unknown_seed_raises(self):
        with self.assertRaises(KeyError):
            self.knn.recommend("tt999")

    def test_titles_without_genres_are_excluded(self):
        enc = PreProcess.encode_title_genres(TITLES)
        enc.loc[enc["t_const"] == "tt6", [c for c in enc.columns if c.startswith("genre_")]] = 0
        self.assertNotIn("tt6", GenreKNN(enc))


if __name__ == "__main__":
    unittest.main()
