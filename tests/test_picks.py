import os
import tempfile
import unittest
from unittest import mock

import pandas as pd

from connectors import EmbyConnector, SQLiteConnector
from ml import ContentKNN, Freshness, PreProcess, Recommender, Requests, UserProfiles
from ml.picks import BACK_CATALOGUE, FRESH, TO_REQUEST
from test_content_knn import CONTENT
from test_freshness import ADDITIONS
from test_tmdb_content import details, response
from utils import Notifications


def watched(imdb_id, user="u1", hours=2.0):
    return {"user_id": user, "user_name": user, "imdb_id": imdb_id, "unit_name": imdb_id, "unit_type": "Movie",
            "items_watched": 1, "total_seconds_watched": hours * 3600, "runtime_seconds": 7200}


class TestRecommenderPicks(unittest.TestCase):

    def setUp(self):
        knn = ContentKNN(CONTENT)
        history = pd.DataFrame([watched("tt3"), watched("tt1"), watched("tt2", user="u2")])
        requests = pd.DataFrame([{"user_id": "u1", "t_const": "tt7", "requested_at": "2026-01-01"}])
        self.rec = Recommender(
            knn=knn, profiles=UserProfiles(knn, history), library={t for t in CONTENT["t_const"] if t in knn},
            freshness=Freshness(ADDITIONS), requests=Requests(requests),
        )

    def test_two_lists_without_overlap(self):
        picks = self.rec.picks("u1", n=2, when="2026-03-01")
        fresh, back = picks[picks["list"] == FRESH], picks[picks["list"] == BACK_CATALOGUE]
        self.assertEqual(fresh["rank"].tolist(), [1, 2])
        self.assertEqual(back["rank"].tolist(), [1, 2])
        self.assertFalse(set(fresh["t_const"]) & set(back["t_const"]))
        self.assertFalse({"tt1", "tt3"} & set(picks["t_const"]))      # watched
        # back catalogue is taste alone: its score is the similarity
        self.assertEqual(back["score"].tolist(), back["similarity"].tolist())

    def test_fresh_puts_new_arrivals_first(self):
        fresh = self.rec.picks("u1", n=3, when="2026-03-01").query("list == @FRESH")
        self.assertEqual(fresh.iloc[0]["t_const"], "tt2")             # added that day
        self.assertEqual(fresh.iloc[0]["added_days_ago"], 0)

    def test_requests_only_boost_the_requester(self):
        mine = self.rec.picks("u1", n=6, when="2026-03-01").set_index("t_const")
        theirs = self.rec.picks("u2", n=6, when="2026-03-01").set_index("t_const")
        self.assertTrue(mine.loc["tt7", "you_requested"])
        self.assertFalse(theirs.loc["tt7", "you_requested"])

    def test_without_dates_only_the_back_catalogue(self):
        self.rec.freshness = Freshness(ADDITIONS.assign(added_at=pd.NaT))
        self.assertEqual(set(self.rec.picks("u1", n=2)["list"]), {BACK_CATALOGUE})

    def test_all_picks_skips_thin_profiles(self):
        self.assertEqual(set(self.rec.all_picks(n=1, when="2026-03-01", min_titles=2)["user_id"]), {"u1"})
        self.assertEqual(set(self.rec.all_picks(n=1, when="2026-03-01", min_titles=1, users={"u2"})["user_id"]), {"u2"})
        self.assertTrue(self.rec.all_picks(min_titles=5).empty)


def item(item_id, item_type, imdb=None, series_id=None, season=None, episode=None, created="2026-01-01"):
    return {"Id": item_id, "Name": item_id, "Type": item_type, "RunTimeTicks": 1500 * 10_000_000, "SeriesId": series_id,
            "SeriesName": "Show" if series_id else None, "ParentIndexNumber": season, "IndexNumber": episode,
            "DateCreated": created, "Genres": [], "ProviderIds": {"Imdb": imdb} if imdb else {}}


class TestToRequest(unittest.TestCase):
    """Titles beyond the server worth requesting: CONTENT split into the library and titles the server doesn't have"""

    BEYOND = {"tt2", "tt4", "tt7"}

    def setUp(self):
        library = CONTENT[~CONTENT["t_const"].isin(self.BEYOND)].reset_index(drop=True)
        knn = ContentKNN(library)
        history = pd.DataFrame([watched("tt3"), watched("tt1"), watched("tt5", user="u2")])
        requests = pd.DataFrame([{"user_id": "u2", "t_const": "tt7", "requested_at": "2026-01-01"}])
        self.rec = Recommender(
            knn=knn, profiles=UserProfiles(knn, history), library=set(library["t_const"]),
            freshness=Freshness(ADDITIONS), requests=Requests(requests), content=library,
            discovery=CONTENT[CONTENT["t_const"].isin(self.BEYOND)].assign(num_votes=50_000).reset_index(drop=True),
        )

    def test_only_titles_beyond_the_server_not_yet_requested(self):
        out = self.rec.to_request("u1", n=5)
        self.assertEqual(out["t_const"].iloc[0], "tt2")         # Heist Two: like the heist and detective titles watched
        self.assertTrue(set(out["t_const"]) <= self.BEYOND - {"tt7"})   # tt7: someone requested it already
        self.assertEqual(out["rank"].tolist(), list(range(1, len(out) + 1)))
        self.assertEqual(set(out["list"]), {TO_REQUEST})

    def test_poorly_rated_or_little_known_titles_are_left_out(self):
        d = self.rec.discovery
        self.rec.discovery = d.assign(avg_rating=d["avg_rating"].where(d["t_const"] != "tt2", 5.0))
        self.assertNotIn("tt2", set(self.rec.to_request("u1")["t_const"]))
        self.rec.discovery = d.assign(num_votes=d["num_votes"].where(d["t_const"] != "tt2", 800))
        self.assertNotIn("tt2", set(self.rec.to_request("u1")["t_const"]))

    def test_picks_add_them_as_a_third_list_and_keep_the_others_on_the_server(self):
        picks = self.rec.picks("u1", n=3, when="2026-03-01", n_request=2)
        self.assertEqual(len(picks[picks["list"] == TO_REQUEST]), min(2, len(self.rec.to_request("u1"))))
        on_server = picks[picks["list"] != TO_REQUEST]
        self.assertTrue(set(on_server["t_const"]) <= self.rec.library)
        self.assertNotIn(TO_REQUEST, set(self.rec.picks("u1", n=3, when="2026-03-01")["list"]))   # only when asked

    def test_the_library_model_stays_library_only(self):
        self.assertNotIn("tt2", self.rec.knn)
        self.assertIn("tt2", self.rec.wide[0])

    def test_nothing_without_discovery_titles(self):
        self.rec.discovery = None
        self.assertTrue(self.rec.to_request("u1").empty)
        self.assertNotIn(TO_REQUEST, set(self.rec.picks("u1", n=2, n_request=3)["list"]))


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


class TestPlaylistItems(TempDBTestCase):

    def items(self, anthologies=()):
        pp = PreProcess("test.db")
        pp._anthologies = lambda: set(anthologies)
        try:
            return pp.get_playlist_items().to_dict()
        finally:
            pp.sqlite._connection.close()

    def test_movies_are_themselves_and_series_their_s1e1(self):
        self.db.ingest_all_library_items(iter([
            item("m-new", "Movie", "tt1", created="2026-05-01"),
            item("m-old", "Movie", "tt1", created="2026-01-01"),        # same movie twice: first added wins
            item("s1", "Series", "tt2"),
            item("s1-special", "Episode", series_id="s1", season=0, episode=1),
            item("s1-e2", "Episode", series_id="s1", season=1, episode=2),
            item("s1-e1", "Episode", series_id="s1", season=1, episode=1),
            item("s1-s2", "Episode", series_id="s1", season=2, episode=1),
            item("s2", "Series", "tt3"),
            item("s2-special", "Episode", series_id="s2", season=0, episode=1),     # only specials
            item("s3", "Series", "tt4"),                                            # no episodes
            item("s4", "Series", "tt5"),
            item("s4-s3e1", "Episode", series_id="s4", season=3, episode=1),        # only a later season
            item("s5", "Series", "tt6"),
            item("s5-s1e4", "Episode", series_id="s5", season=1, episode=4),        # season 1 without its start
        ]))
        self.assertEqual(self.items(), {"tt1": "m-old", "tt2": "s1-e1"})
        # an anthology may start at its earliest season, but never part-way through one
        self.assertEqual(self.items(anthologies={"tt5", "tt6"}), {"tt1": "m-old", "tt2": "s1-e1", "tt5": "s4-s3e1"})

    def test_anthologies_come_from_tmdb_keywords(self):
        self.db.ingest_all_library_items(iter([
            {**item("s1", "Series", "tt1"), "ProviderIds": {"Imdb": "tt1", "Tmdb": "11"}},
            {**item("s2", "Series", "tt2"), "ProviderIds": {"Imdb": "tt2", "Tmdb": "22"}},
        ]))
        keywords = {11: ["anthology", "horror"], 22: ["sitcom"]}
        self.db.ingest_tmdb_title_details(lambda media_type, tmdb_id: details(media_type, tmdb_id, keywords=keywords[tmdb_id]))
        pp = PreProcess("test.db")
        try:
            self.assertEqual(pp._anthologies(), {"tt1"})
        finally:
            pp.sqlite._connection.close()


class TestRecommendationLog(TempDBTestCase):

    def test_runs_are_appended(self):
        picks = pd.DataFrame([
            {"user_id": "u1", "list": FRESH, "rank": 1, "t_const": "tt1", "score": 0.9, "similarity": 0.6,
             "because_you_watched": "Show", "item_id": "m1"},
            {"user_id": "u1", "list": BACK_CATALOGUE, "rank": 1, "t_const": "tt2", "score": 0.5, "similarity": 0.5,
             "because_you_watched": None, "item_id": None},
        ])
        self.assertEqual(self.db.log_recommendations(picks, "ContentKNN", "2026-10-01 03:30:00"), 2)
        self.assertEqual(self.db.log_recommendations(picks, "ContentKNN", "2026-10-02 03:30:00"), 2)
        log = self.db.get_recommendation_log()
        self.assertEqual(len(log), 4)
        self.assertEqual(log.loc[1, ["item_id", "because_you_watched"]].tolist(), [None, None])


class TestSetUserPlaylist(unittest.TestCase):
    """`set_user_playlist` against a fake Emby holding one user's playlists"""

    def setUp(self):
        self.playlists = {"Other": ("p0", ["x"])}       # name -> (id, item IDs)
        self.calls = []
        env = mock.patch.dict(os.environ, {"BASE_DOMAIN": "http://emby", "EMBY_API_KEY": "k"})
        env.start()
        self.addCleanup(env.stop)
        self.emby = EmbyConnector()

    def by_id(self, playlist_id):
        return next(name for name, (pid, _) in self.playlists.items() if pid == playlist_id)

    def get(self, url, params, timeout):
        if url.endswith("/Users/u1/Items"):
            return response(200, {"Items": [{"Name": n, "Id": pid} for n, (pid, _) in self.playlists.items()]})
        pid = url.split("/Playlists/")[1].split("/")[0]
        items = self.playlists[self.by_id(pid)][1]
        return response(200, {"Items": [{"PlaylistItemId": f"e-{i}", "Id": item} for i, item in enumerate(items)]})

    def post(self, url, params, timeout):
        self.calls.append(("post", url.split("/emby")[-1].replace("http://emby", ""), params.get("Ids")))
        if url.endswith("/Playlists"):
            self.playlists[params["Name"]] = (f"p{len(self.playlists)}", params["Ids"].split(","))
        else:
            self.playlists[self.by_id(url.split("/")[-2])][1].extend(params["Ids"].split(","))
        return response(200, {})

    def delete(self, url, params, timeout):
        self.calls.append(("delete", url.replace("http://emby", ""), params.get("EntryIds")))
        name = self.by_id(url.split("/")[-2])
        self.playlists[name][1].clear()
        return response(204)

    def set(self, name, items):
        with mock.patch("connectors.emby_connector.requests.get", side_effect=self.get), \
             mock.patch("connectors.emby_connector.requests.post", side_effect=self.post), \
             mock.patch("connectors.emby_connector.requests.delete", side_effect=self.delete):
            return self.emby.set_user_playlist("u1", name, items)

    def test_create_then_unchanged_then_replace(self):
        self.assertEqual(self.set("Picks", ["a", "b"]), "created")
        self.assertEqual(self.playlists["Picks"][1], ["a", "b"])
        self.assertEqual(self.set("Picks", ["a", "b"]), "unchanged")
        self.assertEqual(self.set("Picks", ["b", "c"]), "updated")      # a new order counts as a change too
        self.assertEqual(self.playlists["Picks"][1], ["b", "c"])
        self.assertEqual(self.playlists["Other"][1], ["x"])              # other playlists are never touched
        self.assertEqual([c[0] for c in self.calls], ["post", "delete", "post"])

    def test_nothing_to_add_does_not_create_an_empty_playlist(self):
        self.assertEqual(self.set("Picks", []), "unchanged")
        self.assertNotIn("Picks", self.playlists)


class TestSplitMessage(unittest.TestCase):

    def test_breaks_between_lines_within_the_limit(self):
        parts = Notifications.split_message(["aaaa", "bbbb", "cc", "d" * 12], limit=10)
        self.assertEqual(parts, ["aaaa\nbbbb", "cc", "d" * 10])


if __name__ == "__main__":
    unittest.main()
