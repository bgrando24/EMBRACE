import os
import tempfile
import unittest

import pandas as pd

from connectors import SQLiteConnector
from ml import PreProcess


def item(item_id, item_type, name, imdb=None, series_id=None, series_name=None):
    return {
        "Id": item_id, "Name": name, "Type": item_type, "RunTimeTicks": 1500 * 10_000_000,
        "SeriesId": series_id, "SeriesName": series_name, "Genres": ["Comedy"],
        "ProviderIds": {"Imdb": imdb} if imdb else {},
    }


def event(item_id, item_type, name, user="u1", date="2026-09-01", duration="1500"):
    return {
        "date": date, "time": "20:00:00", "user_id": user, "item_name": name, "item_id": item_id,
        "item_type": item_type, "duration": duration, "remote_address": "", "user_name": user, "user_has_image": False,
    }


class TestEmbyImdbMapping(unittest.TestCase):

    def setUp(self):
        self._old_cwd = os.getcwd()
        self._tmp = tempfile.TemporaryDirectory()
        os.chdir(self._tmp.name)

        db = SQLiteConnector("test.db")
        db.connect_db()
        # "old server": items that later disappear (Emby IDs changed on rebuild)
        db.ingest_all_library_items(iter([
            item("old-movie", "Movie", "Letterkenny Movie", imdb="tt100"),
            item("old-ep", "Episode", "Old Ep", imdb="tt901", series_id="old-series-id", series_name="Letterkenny"),
            item("gone-ep", "Episode", "Gone Ep", imdb="tt902", series_id="gone-series-id", series_name="Cancelled Show"),
            item("dupe-ep", "Episode", "Dupe Ep", series_id="x", series_name="Twin Name"),
        ]))
        # "current server"
        db.ingest_all_library_items(iter([
            item("new-movie", "Movie", "Letterkenny Movie", imdb="tt100"),
            item("s1", "Series", "Letterkenny", imdb="tt200"),
            item("s2", "Series", "The Survivors", imdb="tt300"),
            item("s3", "Series", "Twin Name", imdb="tt401"),
            item("s4", "Series", "Twin Name", imdb="tt402"),
            item("e1", "Episode", "Ep 1", imdb="tt201", series_id="s1", series_name="Letterkenny"),
            item("e2", "Episode", "Ep 2", imdb="tt202", series_id="s1", series_name="Letterkenny"),
        ]))
        db.ingest_watch_hist_raw_events(lambda d, a: {"u1": [
            event("old-movie", "Movie", "Letterkenny Movie", date="2025-09-01"),
            event("new-movie", "Movie", "Letterkenny Movie"),
            event("e1", "Episode", "Letterkenny - s01e01 - Ep 1"),
            event("e2", "Episode", "Letterkenny - s01e02 - Ep 2"),
            event("old-ep", "Episode", "Letterkenny - s01e03 - Old Ep", date="2025-09-02"),
            # only in watch history, never in library_items
            event("orphan-ep", "Episode", "The Survivors (2025) - s01e04 - Episode 4", date="2025-09-03"),
            event("orphan-movie", "Movie", "1992", date="2025-09-03"),
            event("channel", "TvChannel", "ABC TV", date="2025-09-03"),
            event("gone-ep", "Episode", "Cancelled Show - s01e01 - Gone Ep", date="2025-09-04"),
        ]})
        db._INIT_POPULATE_watch_hist_agg_sessions()
        db._INIT_POPULATE_watch_hist_user_item_stats()
        db._connection.close()

        self.pp = PreProcess("test.db")
        self.pp._mysql_error = ConnectionError("no MySQL in tests")

    def tearDown(self):
        self.pp.sqlite._connection.close()
        os.chdir(self._old_cwd)
        self._tmp.cleanup()

    def mapped(self, use_imdb_episodes=True):
        return self.pp.map_emby_items_to_imdb(use_imdb_episodes).set_index("item_id")

    def test_resolution_paths(self):
        m = self.mapped()
        expected = {
            "new-movie": ("self", "tt100"),
            "old-movie": ("self", "tt100"),
            "s1": ("self", "tt200"),
            "e1": ("series_id", "tt200"),
            "old-ep": ("series_name", "tt200"),
            "orphan-ep": ("series_name", "tt300"),      # parsed from item name, "(2025)" ignored
            "dupe-ep": ("unresolved_series", None),     # two current series share the name
            "gone-ep": ("unresolved_series", None),     # series not in library, MySQL unavailable
            "orphan-movie": ("not_in_library", None),
            "channel": ("not_recommendable", None),
        }
        for item_id, (method, imdb) in expected.items():
            with self.subTest(item_id=item_id):
                self.assertEqual(m.loc[item_id, "match_method"], method)
                self.assertEqual(m.loc[item_id, "imdb_id"] if pd.notna(m.loc[item_id, "imdb_id"]) else None, imdb)

    def test_imdb_episode_fallback(self):
        self.pp.imdb_get_episode_parents = lambda ids: pd.DataFrame(
            [("tt902", "tt500", "Cancelled Show")], columns=["t_const", "parent_t_const", "parent_name"])
        m = self.mapped()
        self.assertEqual(m.loc["gone-ep", "match_method"], "imdb_episode")
        self.assertEqual(m.loc["gone-ep", "imdb_id"], "tt500")
        self.assertEqual(m.loc["gone-ep", "unit_name"], "Cancelled Show")

    def test_user_title_stats_roll_up(self):
        titles, unmatched = self.pp.get_user_title_stats()
        titles = titles.set_index("imdb_id")
        # three Letterkenny episodes (two current, one from the old server) -> one series row
        self.assertEqual(titles.loc["tt200", "items_watched"], 3)
        self.assertEqual(titles.loc["tt200", "unit_type"], "Series")
        # same movie under old and new Emby IDs -> one row, both sessions
        self.assertEqual(titles.loc["tt100", "total_sessions"], 2)
        self.assertEqual(set(unmatched["item_id"]), {"gone-ep", "orphan-movie", "channel"})

    def test_user_aliases_combine_history(self):
        # same person under an old user ID (e.g. before the Emby rebuild), with an old name
        db = self.pp.sqlite
        db.ingest_watch_hist_raw_events(lambda d, a: {"old": [event("old-movie", "Movie", "Letterkenny Movie", user="old-name", date="2025-08-20")]})
        db._connection.execute("UPDATE watch_hist_raw_events SET user_id = 'u0' WHERE user_name = 'old-name'")
        db._connection.commit()
        db._INIT_POPULATE_watch_hist_agg_sessions()
        db._INIT_POPULATE_watch_hist_user_item_stats()

        titles, _ = self.pp.get_user_title_stats()
        self.assertEqual(set(titles["user_id"]), {"u0", "u1"})

        self.assertTrue(db.set_user_alias("u0", "u1"))
        titles, _ = self.pp.get_user_title_stats()
        self.assertEqual(set(titles["user_id"]), {"u1"})
        self.assertEqual(set(titles["user_name"]), {"u1"})    # canonical user's current name
        self.assertEqual(titles.set_index("imdb_id").loc["tt100", "total_sessions"], 3)


class TestUserAliases(unittest.TestCase):

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

    def aliases(self):
        return dict(self.db.get_user_aliases()[["alias_user_id", "user_id"]].itertuples(index=False))

    def test_self_alias_rejected(self):
        self.assertFalse(self.db.set_user_alias("a", "a"))

    def test_canonical_user_cannot_be_an_alias(self):
        self.db.set_user_alias("a", "b")
        self.assertFalse(self.db.set_user_alias("c", "a"))

    def test_existing_aliases_follow_a_newly_aliased_canonical(self):
        self.db.set_user_alias("a", "b")
        self.db.set_user_alias("b", "c")
        self.assertEqual(self.aliases(), {"a": "c", "b": "c"})


if __name__ == "__main__":
    unittest.main()
