import unittest

import numpy as np
import pandas as pd

from ml import ContentKNN, UserProfiles
from ml.evaluate import leave_one_out, popularity_ranker, profile_ranker, summarise, weight_grid


def person(pid, name, role="cast"):
    return {"id": pid, "name": name, "role": role}


def title(t_const, name, votes, genres, keywords=(), people=(), overview="", collection=None, language="en"):
    return {
        "t_const": t_const, "primary_name": name, "type": "movie", "start_year": "2000", "avg_rating": 7.0,
        "num_votes": votes, "genres": list(genres), "keywords": list(keywords), "collection": collection,
        "original_language": language, "people": list(people), "overview": overview,
    }


HEIST = ("heist", "bank robbery")
CONTENT = pd.DataFrame([
    title("tt1", "Heist One", 100, ["Crime"], HEIST, [person(1, "Star"), person(9, "Dir", "director")],
          "a crew plans a bank job", collection="Heist Collection"),
    title("tt2", "Heist Two", 10, ["Crime"], HEIST, [person(1, "Star")], "the crew plans another bank job",
          collection="Heist Collection"),
    title("tt3", "Popular Crime", 10**6, ["Crime"], ["detective"], [person(2, "Other")], "a detective story"),
    title("tt4", "Detective Comedy", 50, ["Comedy"], ["detective", "sitcom"], [person(2, "Other")], "a funny detective"),
    title("tt5", "Sitcom", 500, ["Comedy"], ["sitcom"], [person(3, "Funny")], "a funny family"),
    title("tt6", "Korean Drama", 20, ["Drama"], ["family"], [person(4, "Lead")], "a family story", language="ko"),
    title("tt7", "Korean Thriller", 20, ["Thriller"], ["family"], [person(4, "Lead")], "", language="ko"),
    title("tt8", "Nothing", 5, [], [], [], ""),
])


class TestContentKNN(unittest.TestCase):

    def setUp(self):
        self.knn = ContentKNN(CONTENT)

    def test_shared_keywords_people_and_franchise_beat_genre_and_popularity(self):
        self.assertEqual(self.knn.recommend("tt1", n=2)["t_const"].tolist(), ["tt2", "tt3"])

    def test_titles_without_any_shared_features_are_dropped(self):
        # tt8 has nothing at all; every other title shares at least one token with another
        self.assertNotIn("tt8", self.knn)
        self.assertIn("tt7", self.knn)

    def test_explanation_lists_keywords_and_people_before_genres(self):
        shared = self.knn.recommend("tt1", n=1).loc[0, "shared"]
        self.assertEqual(shared[-1], "Crime")
        self.assertEqual(set(shared[:-1]), {"Star", "heist", "bank robbery", "collection: Heist Collection"})

    def test_language_token_only_for_non_english(self):
        self.assertIn("language: ko", self.knn.recommend("tt6", n=1).loc[0, "shared"])
        self.assertNotIn("language: en", self.knn._features["keywords"])

    def test_tokens_in_one_title_only_are_dropped(self):
        self.assertNotIn("9", self.knn._features["people"])           # the director of tt1 only
        self.assertIn("1", self.knn._features["people"])

    def test_weights(self):
        people_only = ContentKNN(CONTENT, {"genre": 0, "keywords": 0, "people": 1, "description": 0})
        # tt3 and tt4 share only person 2
        self.assertAlmostEqual(people_only.recommend("tt3", n=1).loc[0, "similarity"], 1.0, places=5)
        with self.assertRaises(ValueError):
            ContentKNN(CONTENT, {"plot": 1.0})
        with self.assertRaises(ValueError):
            ContentKNN(CONTENT, {b: 0 for b in ContentKNN.DEFAULT_WEIGHTS})

    def test_user_profiles_explain_with_shared_features(self):
        history = pd.DataFrame([{
            "user_id": "u1", "user_name": "u1", "imdb_id": "tt1", "unit_name": "Heist One", "unit_type": "Movie",
            "items_watched": 1, "total_seconds_watched": 7200, "runtime_seconds": 7200,
        }])
        recs = UserProfiles(self.knn, history).recommend("u1", n=1)
        self.assertEqual(recs.loc[0, ["t_const", "because_you_watched"]].tolist(), ["tt2", "Heist One"])
        self.assertIn("heist", recs.loc[0, "shared"])


class TestEvaluate(unittest.TestCase):

    def history(self):
        def row(imdb_id, hours):
            return {"user_id": "u1", "user_name": "u1", "imdb_id": imdb_id, "unit_name": imdb_id, "unit_type": "Movie",
                    "items_watched": 1, "total_seconds_watched": hours * 3600, "runtime_seconds": 2 * 3600}
        # liked tt1 and tt2 (same franchise), opened and abandoned tt5
        return pd.DataFrame([row("tt1", 2), row("tt2", 2), row("tt5", 0.05)])

    def test_leave_one_out_ranks_held_out_title_and_excludes_the_rest(self):
        knn = ContentKNN(CONTENT)
        scope = {t for t in CONTENT["t_const"] if t in knn}
        res = leave_one_out(profile_ranker(knn), self.history(), scope, scope).set_index("imdb_id")
        self.assertEqual(sorted(res.index), ["tt1", "tt2"])        # abandoned tt5 is never held out
        self.assertEqual(res["rank"].tolist(), [1, 1])            # each half of the franchise finds the other
        self.assertEqual(res["n_ranked"].tolist(), [len(scope) - 2] * 2)   # minus the other two watched titles

    def test_popularity_and_summary(self):
        votes = CONTENT.set_index("t_const")["num_votes"]
        scope = set(CONTENT["t_const"])
        res = leave_one_out(popularity_ranker(votes), self.history(), scope, scope).set_index("imdb_id")
        # by votes: tt3, tt5 (excluded), tt1, tt4, ... -> tt1 is 2nd, tt2 (10 votes) is 5th
        self.assertEqual(res["rank"].to_dict(), {"tt1": 2, "tt2": 5})
        s = summarise(res.reset_index(), k=2)
        self.assertEqual(s["hit@2"], 0.5)
        self.assertAlmostEqual(s["mrr"], (1 / 2 + 1 / 5) / 2)
        n = len(scope) - 2
        self.assertAlmostEqual(s["auc"], np.mean([(n - 2) / (n - 1), (n - 5) / (n - 1)]))

    def test_weight_grid_sums_to_one(self):
        grid = weight_grid(0.5)
        self.assertEqual(len(grid), 10)
        self.assertTrue(all(abs(sum(w.values()) - 1) < 1e-9 for w in grid))


if __name__ == "__main__":
    unittest.main()
