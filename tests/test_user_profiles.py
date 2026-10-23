import unittest

import numpy as np
import pandas as pd

from ml import GenreKNN, PreProcess, UserProfiles, title_interest
from test_genre_knn import rows

TITLES = rows(
    ("tt1", "Crime Show", "tvSeries", 100, ["Crime", "Drama"]),
    ("tt2", "Romcom", "movie", 100, ["Comedy", "Romance"]),
    ("tt3", "Crime Drama Popular", "movie", 900, ["Crime", "Drama"]),
    ("tt4", "Crime Drama Niche", "movie", 10, ["Crime", "Drama"]),
    ("tt5", "Comedy Popular", "movie", 5000, ["Comedy"]),
    ("tt6", "Horror Movie", "movie", 10**6, ["Horror"]),
    ("tt7", "Abandoned Horror", "movie", 50, ["Horror"]),
    ("tt8", "Crime Comedy", "movie", 50, ["Comedy", "Crime"]),
)


def user_title(imdb_id, name, unit_type, hours, runtime_hours=None, user="u1"):
    return {
        "user_id": user, "user_name": user, "imdb_id": imdb_id, "unit_name": name, "unit_type": unit_type,
        "items_watched": 1, "total_seconds_watched": hours * 3600,
        "runtime_seconds": runtime_hours * 3600 if runtime_hours is not None else np.nan,
    }


class TestTitleInterest(unittest.TestCase):

    def interest(self, *user_titles):
        return title_interest(pd.DataFrame(user_titles)).round(4).tolist()

    def test_movie_is_fraction_of_runtime_watched_over_all_sessions(self):
        self.assertEqual(self.interest(user_title("tt2", "M", "Movie", 1.0, runtime_hours=2.0)), [0.5])

    def test_movie_rewatch_does_not_count_double(self):
        self.assertEqual(self.interest(user_title("tt2", "M", "Movie", 5.0, runtime_hours=2.0)), [1.0])

    def test_movie_without_runtime_falls_back_to_two_hours(self):
        self.assertEqual(self.interest(user_title("tt2", "M", "Movie", 1.0), user_title("tt3", "M", "Movie", 1.0, 0)), [0.5, 0.5])

    def test_series_is_log_of_hours_with_two_hours_equal_to_one_movie(self):
        self.assertEqual(self.interest(*(user_title("tt1", "S", "Series", h) for h in (0, 2, 6, 14))), [0, 1, 2, 3])


class TestGenreKNNProfile(unittest.TestCase):

    def setUp(self):
        self.knn = GenreKNN(PreProcess.encode_title_genres(TITLES))

    def test_profile_of_one_title_ranks_like_recommend(self):
        profile = self.knn.profile_vector(["tt1"], [1.0])
        by_profile = self.knn.recommend_for_profile(profile, n=4, exclude=["tt1"])["t_const"].tolist()
        self.assertEqual(by_profile, self.knn.recommend("tt1", n=4)["t_const"].tolist())

    def test_weights_shift_the_profile(self):
        def top(weights):
            profile = self.knn.profile_vector(["tt1", "tt2"], weights)
            return self.knn.recommend_for_profile(profile, n=1, exclude=["tt1", "tt2"])["t_const"][0]
        self.assertEqual(top([3.0, 1.0]), "tt3")    # mostly crime drama -> the more popular crime drama
        self.assertEqual(top([1.0, 3.0]), "tt5")    # mostly romcom -> the comedy

    def test_exclude_and_unknown_or_zero_weight_titles(self):
        profile = self.knn.profile_vector(["tt1", "tt-unknown", "tt6"], [1.0, 5.0, 0.0])
        recs = self.knn.recommend_for_profile(profile, n=10, exclude=["tt1", "tt3"])
        self.assertNotIn("tt1", set(recs["t_const"]))
        self.assertEqual(recs["t_const"].tolist()[:1], ["tt4"])
        self.assertEqual(recs.loc[0, "genres"], ["Crime", "Drama"])

    def test_profile_with_nothing_usable_raises(self):
        with self.assertRaises(ValueError):
            self.knn.profile_vector(["tt-unknown", "tt2"], [1.0, 0.0])


class TestUserProfiles(unittest.TestCase):

    def setUp(self):
        self.knn = GenreKNN(PreProcess.encode_title_genres(TITLES))
        history = pd.DataFrame([
            user_title("tt1", "Crime Show", "Series", 14),                    # interest 3
            user_title("tt2", "Romcom", "Movie", 1.5, runtime_hours=1.5),  # interest 1
            user_title("tt7", "Abandoned Horror", "Movie", 0.05, runtime_hours=2),  # opened and abandoned
            user_title("tt-tvmovie", "Not In Model", "Movie", 2, runtime_hours=2),
            user_title("tt5", "Comedy Popular", "Movie", 2, runtime_hours=2, user="u2"),
        ])
        self.profiles = UserProfiles(self.knn, history)

    def test_profile_titles_skip_abandoned_and_unknown(self):
        self.assertEqual(self.profiles.profile_titles("u1")["imdb_id"].tolist(), ["tt1", "tt2"])
        users = self.profiles.users().set_index("user_id")
        self.assertEqual(users.loc["u1", ["titles_watched", "titles_in_profile"]].tolist(), [4, 2])

    def test_genre_shares(self):
        # Crime Show (interest 3) splits over Crime + Drama, Romcom (1) over Comedy + Romance
        self.assertEqual(self.profiles.genre_shares("u1").round(3).to_dict(),
                         {"Crime": 0.375, "Drama": 0.375, "Comedy": 0.125, "Romance": 0.125})

    def test_recommend_excludes_everything_watched_including_abandoned(self):
        recs = self.profiles.recommend("u1", n=10)
        self.assertTrue({"tt1", "tt2", "tt7"}.isdisjoint(recs["t_const"]))
        self.assertEqual(recs["t_const"].tolist()[:2], ["tt3", "tt4"])
        self.assertIn("tt5", set(recs["t_const"]))    # watched by another user only

    def test_because_you_watched_is_the_most_similar_title(self):
        recs = self.profiles.recommend("u1", n=10).set_index("t_const")["because_you_watched"]
        self.assertEqual(recs["tt3"], "Crime Show")
        self.assertEqual(recs["tt5"], "Romcom")    # not Crime Show, despite its higher interest
        # Crime Comedy is equally similar to both: the tie goes to the higher-interest title
        self.assertEqual(recs["tt8"], "Crime Show")
        # nothing in common with anything watched: no explanation
        self.assertIsNone(recs["tt6"])

    def test_allowed_filter_and_empty_result(self):
        self.assertEqual(self.profiles.recommend("u1", allowed=["tt5", "tt6"])["t_const"].tolist(), ["tt5", "tt6"])
        empty = self.profiles.recommend("u1", allowed=["tt1"])
        self.assertTrue(empty.empty)
        self.assertIn("because_you_watched", empty.columns)

    def test_unknown_user_raises(self):
        with self.assertRaises(KeyError):
            self.profiles.recommend("nobody")


if __name__ == "__main__":
    unittest.main()
