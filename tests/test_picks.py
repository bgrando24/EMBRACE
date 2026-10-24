import os
import tempfile
import unittest
from unittest import mock

import pandas as pd

from connectors import EmbyConnector, SQLiteConnector
from ml import ContentKNN, Freshness, PreProcess, Recommender, Requests, UserProfiles
from ml.picks import BACK_CATALOGUE, FRESH
from test_content_knn import CONTENT
from test_freshness import ADDITIONS
from test_tmdb_content import response
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

    def test_movies_are_themselves_and_series_their_first_episode(self):
        self.db.ingest_all_library_items(iter([
            item("m-new", "Movie", "tt1", created="2026-05-01"),
            item("m-old", "Movie", "tt1", created="2026-01-01"),        # same movie twice: first added wins
            item("s1", "Series", "tt2"),
            item("s1-special", "Episode", series_id="s1", season=0, episode=1),
            item("s1-e2", "Episode", series_id="s1", season=1, episode=2),
            item("s1-e1", "Episode", series_id="s1", season=1, episode=1),
            item("s1-s2", "Episode", series_id="s1", season=2, episode=1),
            item("s2", "Series", "tt3"),
            item("s2-special", "Episode", series_id="s2", season=0, episode=1),     # only specials: use them
            item("s3", "Series", "tt4"),                                            # no episodes: not playable
        ]))
        pp = PreProcess("test.db")
        try:
            items = pp.get_playlist_items()
        finally:
            pp.sqlite._connection.close()
        self.assertEqual(items.to_dict(), {"tt1": "m-old", "tt2": "s1-e1", "tt3": "s2-special"})


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
