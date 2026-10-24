import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import numpy as np
import pandas as pd
import requests

from connectors import EmbyConnector, JellyseerrConnector, SQLiteConnector, TMDBConnector
from ml import ContentKNN, PreProcess
from ml.evaluate import leave_one_out, popularity_ranker
from test_content_knn import CONTENT
from test_tmdb_content import FakeTMDB, item, response


class TempDBTestCase(unittest.TestCase):

    def setUp(self):
        self._old_cwd = os.getcwd()
        self._tmp = tempfile.TemporaryDirectory()
        os.chdir(self._tmp.name)
        self.db = SQLiteConnector("test.db")
        self.db.connect_db()

    def tearDown(self):
        self.db._connection.close()
        os.chdir(self._old_cwd)
        self._tmp.cleanup()


def state(item_id, item_type="Movie", played=True):
    return {"item_id": item_id, "item_type": item_type, "played": played, "play_count": 0, "last_played": None,
            "is_favorite": False, "position_ticks": 0}


class TestEmbyUserItemStates(TempDBTestCase):

    def test_get_user_item_states_merges_filters_and_pages(self):
        pages = {
            ("IsPlayed", 0): {"Items": [{"Id": "a", "Type": "Movie", "UserData": {"Played": True, "PlayCount": 2}}], "TotalRecordCount": 2},
            ("IsPlayed", 1): {"Items": [{"Id": "b", "Type": "Episode", "UserData": {"Played": True}}], "TotalRecordCount": 2},
            ("IsFavorite", 0): {"Items": [{"Id": "a", "Type": "Movie", "UserData": {"Played": True, "PlayCount": 2, "IsFavorite": True}}], "TotalRecordCount": 1},
            ("IsResumable", 0): {"Items": [], "TotalRecordCount": 0},
        }
        get = lambda url, params, timeout: response(200, pages[(params["Filters"], params["StartIndex"])])
        with mock.patch.dict(os.environ, {"BASE_DOMAIN": "http://emby", "EMBY_API_KEY": "k"}), \
             mock.patch("connectors.emby_connector.requests.get", side_effect=get):
            states = {s["item_id"]: s for s in EmbyConnector().get_user_item_states("u1", page_size=1)}
        self.assertEqual(set(states), {"a", "b"})
        self.assertEqual((states["a"]["play_count"], states["a"]["is_favorite"]), (2, True))
        self.assertEqual((states["b"]["item_type"], states["b"]["played"]), ("Episode", True))

    def test_snapshot_is_replaced_and_kept_on_failure(self):
        users = lambda: {"one": "u1", "two": "u2"}
        self.assertEqual(self.db.ingest_user_item_states(users, lambda u: [state(f"{u}-m1"), state(f"{u}-m2")]), 4)
        self.assertEqual(self.db.ingest_user_item_states(users, lambda u: [state(f"{u}-m3")]), 2)
        self.assertEqual(set(self.db.get_user_item_states()["item_id"]), {"u1-m3", "u2-m3"})

        def broken(u):
            if u == "u2":
                raise requests.HTTPError("500")
            return [state("new")]
        self.assertEqual(self.db.ingest_user_item_states(users, broken), -1)
        self.assertEqual(set(self.db.get_user_item_states()["item_id"]), {"u1-m3", "u2-m3"})


class TestPlayedTitles(TempDBTestCase):

    def setUp(self):
        super().setUp()
        self.db.ingest_all_library_items(iter([
            item("m1", "Movie", "Watched Movie", imdb="tt1"),
            item("m2", "Movie", "Played Movie", imdb="tt2"),
            item("s1", "Series", "Show", imdb="tt3"),
            item("e1", "Episode", "Ep 1", series_id="s1", series_name="Show"),
            item("e2", "Episode", "Ep 2", series_id="s1", series_name="Show"),
        ]))
        self.db.ingest_watch_hist_raw_events(lambda d, a: {"u1": [{
            "date": "2026-09-01", "time": "20:00:00", "user_id": "u1", "item_name": "Watched Movie", "item_id": "m1",
            "item_type": "Movie", "duration": "1500", "remote_address": "", "user_name": "one", "user_has_image": False,
        }]})
        self.db._INIT_POPULATE_watch_hist_agg_sessions()
        self.db._INIT_POPULATE_watch_hist_user_item_stats()
        self.db.ingest_user_item_states(lambda: {"one": "u1"}, lambda u: [
            state("m1"), state("m2"), state("e1", "Episode"), state("e2", "Episode"), state("s1", "Series"),
            state("unplayed", played=False),
        ])
        self.pp = PreProcess("test.db")
        self.pp._mysql_error = ConnectionError("no MySQL in tests")

    def tearDown(self):
        self.pp.sqlite._connection.close()
        super().tearDown()

    def test_played_titles_not_in_history(self):
        titles, _ = self.pp.get_user_title_stats()
        played = self.pp.get_played_titles(titles).set_index("imdb_id")
        self.assertEqual(sorted(played.index), ["tt2", "tt3"])           # tt1 is already in the watch history
        self.assertEqual(played.loc["tt2", ["unit_type", "total_seconds_watched", "runtime_seconds"]].tolist(), ["Movie", 1500, 1500])
        self.assertEqual(played.loc["tt3", ["items_watched", "total_seconds_watched"]].tolist(), [2, 3000])   # 2 episodes
        self.assertEqual(set(played["source"]), {"emby_played"})
        self.assertEqual(set(played["user_name"]), {"one"})

    def test_users_without_history_get_their_emby_name(self):
        self.db.ingest_user_item_states(lambda: {"Newbie": "u9"}, lambda u: [state("m2")])
        titles, _ = self.pp.get_user_title_stats()
        played = self.pp.get_played_titles(titles)
        self.assertEqual(played.set_index("user_id").loc["u9", "user_name"], "Newbie")

    def test_played_titles_fill_profiles_but_are_never_held_out(self):
        rows = pd.DataFrame([
            {"user_id": "u1", "user_name": "u1", "imdb_id": t, "unit_name": t, "unit_type": "Movie", "items_watched": 1,
             "total_seconds_watched": 7200, "runtime_seconds": 7200, "source": src}
            for t, src in [("tt1", None), ("tt2", None), ("tt3", "emby_played")]
        ])
        votes = CONTENT.set_index("t_const")["num_votes"]
        scope = set(CONTENT["t_const"])
        res = leave_one_out(popularity_ranker(votes), rows, scope, scope)
        self.assertEqual(sorted(res["imdb_id"]), ["tt1", "tt2"])


class TestTMDBRecommendations(TempDBTestCase):

    def test_parsed_with_media_type_defaulting_to_the_title(self):
        tmdb = TMDBConnector("token")
        tmdb._session = mock.Mock()
        tmdb._session.get.return_value = response(200, {
            "id": 1, "title": "M", "genres": [], "keywords": {"keywords": []}, "credits": {"cast": [], "crew": []},
            "recommendations": {"results": [{"id": 5, "media_type": "movie"}, {"id": 6}]},
        })
        self.assertEqual(tmdb.fetch_title_details("movie", 1)["recommendations"], ["movie:5", "movie:6"])

    def test_old_table_is_migrated_and_refetched(self):
        self.db.ingest_all_library_items(iter([item("m1", "Movie", "M", imdb="tt1", tmdb="101")]))
        self.db._connection.execute("""CREATE TABLE tmdb_title_details(media_type TEXT NOT NULL, tmdb_id INTEGER NOT NULL,
            found INTEGER NOT NULL, imdb_id TEXT, title TEXT, overview TEXT, original_language TEXT, genres TEXT,
            keywords TEXT, collection TEXT, people TEXT, fetched_at TEXT NOT NULL, PRIMARY KEY (media_type, tmdb_id))""")
        self.db._connection.execute("""INSERT INTO tmdb_title_details VALUES ('movie', 101, 1, 'tt1', 'M', '', 'en',
            '[]', '[]', NULL, '[]', '2999-01-01 00:00:00')""")
        tmdb = FakeTMDB()
        self.assertEqual(self.db.get_tmdb_title_details().loc[0, "recommendations"], [])
        self.assertEqual(self.db.ingest_tmdb_title_details(tmdb.fetch), 1)      # not stale, but no recommendations yet
        self.assertEqual(self.db.ingest_tmdb_title_details(tmdb.fetch), 0)

    def test_tmdb_recs_block_links_co_recommended_titles(self):
        content = CONTENT.assign(
            media_type="movie", tmdb_id=range(1, len(CONTENT) + 1),
            recommendations=[["movie:99"], [], [], [], [], ["movie:99"], [], []],
        )
        recs_only = ContentKNN(content, {b: float(b == "tmdb_recs") for b in ContentKNN.DEFAULT_WEIGHTS})
        # tt1 and tt6 share nothing but TMDB recommending movie 99 for both
        self.assertEqual(sorted(recs_only.titles["t_const"]), ["tt1", "tt6"])
        self.assertAlmostEqual(recs_only.recommend("tt1", n=1).loc[0, "similarity"], 1.0, places=5)


class TestMovieLens(unittest.TestCase):

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        d = Path(self._tmp.name)
        pd.DataFrame({"movieId": [1, 2, 3], "imdbId": [111, 2222222, 33], "tmdbId": [0, 0, 0]}).to_csv(d / "links.csv", index=False)
        pd.DataFrame({
            "userId": [1, 2, 3, 1, 2, 1, 3], "movieId": [1, 1, 1, 2, 2, 3, 3],
            "rating": [5.0, 4.0, 2.0, 4.5, 5.0, 3.5, 4.0], "timestamp": 0,
        }).to_csv(d / "ratings.csv", index=False)
        self.dir = d

    def tearDown(self):
        self._tmp.cleanup()

    def test_likers_keyed_by_padded_imdb_id_and_cached(self):
        cache = str(self.dir / "cache.parquet")
        likers = PreProcess.get_movielens_likers(str(self.dir), cache_path=cache)
        self.assertEqual({t: sorted(u) for t, u in likers.items()}, {"tt0000111": [1, 2], "tt2222222": [1, 2], "tt0000033": [3]})
        (self.dir / "ratings.csv").unlink()      # the cache is used from now on
        self.assertEqual(len(PreProcess.get_movielens_likers(str(self.dir), cache_path=cache)), 3)

    def test_movielens_block_is_collaborative_and_optional(self):
        likers = [np.array([1, 2]), np.array([1, 2]), None, None, None, np.array([3]), None, None]
        knn = ContentKNN(CONTENT.assign(movielens_likers=likers), {b: float(b == "movielens") for b in ContentKNN.DEFAULT_WEIGHTS})
        # user 3 likes only one title, so is dropped; tt1 and tt2 are liked by the same users
        self.assertEqual(sorted(knn.titles["t_const"]), ["tt1", "tt2"])
        self.assertEqual(knn.recommend("tt1", n=1).loc[0, "t_const"], "tt2")
        # without MovieLens data the default model still works
        self.assertIn("tt1", ContentKNN(CONTENT))


def jellyseerr_request(rid, tmdb_id, media_type="movie", user=None, created="2026-09-01T10:00:00.000Z"):
    return {"id": rid, "status": 2, "type": media_type, "createdAt": created, "updatedAt": created,
            "media": {"tmdbId": tmdb_id, "mediaType": media_type, "status": 5}, "requestedBy": user or {}}


class TestJellyseerr(TempDBTestCase):

    def test_requires_configuration(self):
        with mock.patch.dict(os.environ, {"JELLYSEERR_URL": "", "JELLYSEERR_API_KEY": ""}):
            with self.assertRaises(ValueError):
                JellyseerrConnector()

    def test_iter_requests_pages_and_parses(self):
        j = JellyseerrConnector("http://seerr/", "key")
        j._session = mock.Mock()
        j._session.get.side_effect = [
            response(200, {"pageInfo": {"results": 3}, "results": [
                jellyseerr_request(1, 10, user={"id": 7, "displayName": "Beech", "jellyfinUserId": "emby-b"}),
                jellyseerr_request(2, 20, "tv", user={"id": 8, "jellyfinUsername": "grbic"}),
            ]}),
            response(200, {"pageInfo": {"results": 3}, "results": [{"id": 3, "type": "movie", "media": {}}]}),
        ]
        reqs = list(j.iter_requests(page_size=2))
        self.assertEqual(j._session.get.call_args_list[1].kwargs["params"]["skip"], 2)
        self.assertEqual([r["request_id"] for r in reqs], [1, 2])     # no TMDB ID -> skipped
        self.assertEqual((reqs[0]["status"], reqs[0]["media_status"], reqs[0]["requester_emby_id"]), ("approved", "available", "emby-b"))
        self.assertEqual((reqs[1]["media_type"], reqs[1]["requester_name"], reqs[1]["requester_emby_id"]), ("tv", "grbic", None))

    def test_requests_link_to_users_and_titles(self):
        self.db.ingest_all_library_items(iter([
            item("m1", "Movie", "M", imdb="tt1", tmdb="10"), item("s1", "Series", "S", imdb="tt2", tmdb="20"),
        ]))
        self.db.ingest_tmdb_title_details(FakeTMDB().fetch)
        for uid, name in [("emby-b", "beech"), ("emby-g", "grbic"), ("old-g", "grbic-old")]:
            self.db.ingest_watch_hist_raw_events(lambda d, a, uid=uid, name=name: {name: [{
                "date": "2026-09-01", "time": "20:00:00", "user_id": uid, "item_name": "M", "item_id": "m1",
                "item_type": "Movie", "duration": "100", "remote_address": "", "user_name": name, "user_has_image": False,
            }]})
        self.db.set_user_alias("old-g", "emby-g")
        rows = [
            jellyseerr_request(1, 10, user={"displayName": "Beech", "jellyfinUserId": "emby-b"}),
            jellyseerr_request(2, 20, "tv", user={"displayName": "GRBIC-OLD"}),           # by name, then alias
            jellyseerr_request(3, 999, user={"displayName": "stranger"}),
        ]
        j = JellyseerrConnector("http://seerr", "key")
        j._session = mock.Mock()
        j._session.get.return_value = response(200, {"pageInfo": {"results": 3}, "results": rows})
        self.assertEqual(self.db.ingest_jellyseerr_requests(j.iter_requests), 3)
        self.assertEqual(self.db.ingest_jellyseerr_requests(j.iter_requests), 3)      # upsert, no duplicates
        self.assertEqual(len(self.db.get_jellyseerr_requests()), 3)

        pp = PreProcess("test.db")
        try:
            req = pp.get_user_requests().set_index("tmdb_id")
        finally:
            pp.sqlite._connection.close()
        self.assertEqual(req.loc[10, ["user_id", "t_const"]].tolist(), ["emby-b", "tt1"])
        self.assertEqual(req.loc[20, ["user_id", "t_const"]].tolist(), ["emby-g", "tt2"])
        self.assertTrue(pd.isna(req.loc[999, "user_id"]) and pd.isna(req.loc[999, "t_const"]))
        self.assertEqual(req.loc[10, "requested_at"], pd.Timestamp("2026-09-01 10:00:00"))

    def test_failed_fetch_keeps_table(self):
        def broken():
            yield jellyseerr_request(1, 10)
            raise requests.HTTPError("401")
        self.assertEqual(self.db.ingest_jellyseerr_requests(broken), -1)
        self.assertEqual(len(self.db.get_jellyseerr_requests()), 0)


if __name__ == "__main__":
    unittest.main()


class TestRequestSignals(unittest.TestCase):

    def requests(self):
        return pd.DataFrame([
            {"user_id": "u1", "t_const": "tt2", "requested_at": pd.Timestamp("2026-01-10")},
            {"user_id": "u2", "t_const": "tt5", "requested_at": pd.Timestamp("2026-01-10")},
            {"user_id": "u1", "t_const": "tt4", "requested_at": pd.Timestamp("2026-03-01")},
            {"user_id": "u1", "t_const": None, "requested_at": pd.Timestamp("2026-01-01")},     # never reached the library
        ])

    def test_boost_own_requests_made_before_then(self):
        from ml import Requests
        r = Requests(self.requests())
        self.assertEqual(r.boost("2026-02-01", "u1", own=0.1).to_dict(), {"tt2": 0.1})
        self.assertEqual(r.boost("2026-02-01", "u1", own=0.1, others=0.05).to_dict(), {"tt2": 0.1, "tt5": 0.05})
        self.assertEqual(set(r.boost(None, "u1").index), {"tt2", "tt4"})
        self.assertTrue(r.boost(None, "nobody").empty)

    def rows(self):
        def row(t, first, source=None, requested=None):
            return {"user_id": "u1", "user_name": "u1", "imdb_id": t, "unit_name": t, "unit_type": "Movie",
                    "items_watched": 1, "total_seconds_watched": 7200, "runtime_seconds": 7200,
                    "first_watched_timestamp": first, "source": source, "requested_at": requested}
        return pd.DataFrame([
            row("tt1", "2026-01-05 20:00:00"),
            row("tt3", "2026-02-02 20:00:00"),
            # requested, not watched: joins the profile from when it was requested, never excluded
            row("tt4", None, "jellyseerr_request", pd.Timestamp("2026-01-20")),
        ])

    def test_requests_are_not_seen_and_join_profiles_from_their_date(self):
        from test_freshness import ADDITIONS
        from ml import Freshness
        from ml.evaluate import next_watch
        f = Freshness(ADDITIONS)
        scope = set(CONTENT["t_const"])
        calls = []

        def ranker(t_consts, weights, exclude, candidates, when, user_id):
            calls.append((sorted(t_consts), exclude, set(candidates), user_id))
            return sorted(candidates)
        next_watch(ranker, self.rows(), scope, f)
        (profile, exclude, candidates, user_id), = calls           # only tt3 can be held out (tt1 is the first watch)
        self.assertEqual(profile, ["tt1", "tt4"])
        self.assertEqual(exclude, {"tt1"})
        self.assertIn("tt4", candidates)
        self.assertEqual(user_id, "u1")

    def test_user_profiles_still_recommend_requested_titles(self):
        from ml import UserProfiles
        rows = self.rows().drop(columns=["first_watched_timestamp", "requested_at"])
        recs = UserProfiles(ContentKNN(CONTENT), rows).recommend("u1", n=10)
        self.assertIn("tt4", set(recs["t_const"]))
        self.assertTrue({"tt1", "tt3"}.isdisjoint(recs["t_const"]))


class TestRequestedTitles(TempDBTestCase):

    def test_requested_unwatched_titles_become_profile_rows(self):
        from ml import title_interest
        self.db.ingest_all_library_items(iter([
            item("m1", "Movie", "M", imdb="tt1", tmdb="10"), item("s1", "Series", "S", imdb="tt2", tmdb="20"),
        ]))
        self.db.ingest_tmdb_title_details(FakeTMDB().fetch)
        self.db.ingest_watch_hist_raw_events(lambda d, a: {"u": [{
            "date": "2026-09-01", "time": "20:00:00", "user_id": "u1", "item_name": "M", "item_id": "m1",
            "item_type": "Movie", "duration": "100", "remote_address": "", "user_name": "one", "user_has_image": False,
        }]})
        self.db._INIT_POPULATE_watch_hist_agg_sessions()
        self.db._INIT_POPULATE_watch_hist_user_item_stats()
        j = JellyseerrConnector("http://seerr", "key")
        j._session = mock.Mock()
        j._session.get.return_value = response(200, {"pageInfo": {"results": 2}, "results": [
            jellyseerr_request(1, 10, user={"jellyfinUserId": "u1"}),           # already watched -> not a request row
            jellyseerr_request(2, 20, "tv", user={"jellyfinUserId": "u1"}),
        ]})
        self.db.ingest_jellyseerr_requests(j.iter_requests)
        pp = PreProcess("test.db")
        pp._mysql_error = ConnectionError("no MySQL in tests")
        try:
            titles, _ = pp.get_user_title_stats()
            req = pp.get_requested_titles(titles, interest=0.5)
        finally:
            pp.sqlite._connection.close()
        self.assertEqual(req[["user_id", "imdb_id", "unit_type", "unit_name", "source"]].values.tolist(),
                         [["u1", "tt2", "Series", "Title 20", "jellyseerr_request"]])
        self.assertAlmostEqual(title_interest(req).iloc[0], 0.5)
