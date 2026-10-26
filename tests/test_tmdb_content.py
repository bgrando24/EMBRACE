import os
import tempfile
import unittest
from unittest import mock

import pandas as pd
import requests

from connectors import SQLiteConnector, TMDBConnector
from ml import PreProcess


def item(item_id, item_type, name, imdb=None, tmdb=None, series_id=None, series_name=None):
    providers = {k: v for k, v in {"Imdb": imdb, "Tmdb": tmdb}.items() if v}
    return {
        "Id": item_id, "Name": name, "Type": item_type, "RunTimeTicks": 1500 * 10_000_000,
        "SeriesId": series_id, "SeriesName": series_name, "Genres": ["Comedy"], "ProviderIds": providers,
    }


def details(media_type, tmdb_id, imdb_id=None, keywords=("k1",)):
    return {
        "tmdb_id": tmdb_id, "media_type": media_type, "imdb_id": imdb_id, "title": f"Title {tmdb_id}",
        "overview": "An overview.", "original_language": "en", "genres": ["Comedy"], "keywords": list(keywords),
        "collection": None, "people": [{"id": 1, "name": "Someone", "role": "cast"}],
    }


class FakeTMDB:
    """Records calls; `missing` IDs return None (TMDB 404), `broken` IDs raise, `recs` are TMDB recommendations"""

    def __init__(self, missing=(), broken=(), finds=None, recs=None):
        self.calls, self.find_calls = [], []
        self.missing, self.broken, self.finds, self.recs = set(missing), set(broken), finds or {}, recs or {}

    def fetch(self, media_type, tmdb_id):
        self.calls.append((media_type, tmdb_id))
        if tmdb_id in self.broken:
            raise requests.HTTPError("500")
        if tmdb_id in self.missing:
            return None
        return {**details(media_type, tmdb_id, imdb_id=f"tt-tmdb-{tmdb_id}"), "recommendations": self.recs.get(tmdb_id, [])}

    def find(self, imdb_id):
        self.find_calls.append(imdb_id)
        return self.finds.get(imdb_id)


class TMDBDetailsTestCase(unittest.TestCase):

    def setUp(self):
        self._old_cwd = os.getcwd()
        self._tmp = tempfile.TemporaryDirectory()
        os.chdir(self._tmp.name)
        self.db = SQLiteConnector("test.db")
        self.db.connect_db()
        self.db.ingest_all_library_items(iter([
            item("m1", "Movie", "Movie One", imdb="tt1", tmdb="101"),
            item("s1", "Series", "Show One", imdb="tt2", tmdb="202"),
            item("m2", "Movie", "No TMDB", imdb="tt3"),
            # episodes of a show with no series row (removed, or its Emby ID changed on the rebuild)
            item("e1", "Episode", "Ep 1", imdb="tt-ep1", series_id="gone", series_name="Gone Show"),
            item("e2", "Episode", "Ep 2", imdb="tt-ep2", series_id="gone", series_name="Gone Show"),
            # episode of a linked show: no lookup needed
            item("e3", "Episode", "Ep 3", imdb="tt-ep3", series_id="s1", series_name="Show One"),
        ]))

    def tearDown(self):
        self.db._connection.close()
        os.chdir(self._old_cwd)
        self._tmp.cleanup()

    def query(self, sql):
        return self.db._connection.execute(sql).fetchall()


class TestIngestTMDBTitleDetails(TMDBDetailsTestCase):

    def test_fetches_library_movies_and_series_once(self):
        tmdb = FakeTMDB()
        self.assertEqual(self.db.ingest_tmdb_title_details(tmdb.fetch), 2)
        self.assertEqual(sorted(tmdb.calls), [("movie", 101), ("tv", 202)])
        self.assertEqual(self.db.ingest_tmdb_title_details(tmdb.fetch), 0)
        self.assertEqual(len(tmdb.calls), 2)

    def test_stale_details_are_refetched(self):
        tmdb = FakeTMDB()
        self.db.ingest_tmdb_title_details(tmdb.fetch)
        self.db._connection.execute("UPDATE tmdb_title_details SET fetched_at = '2000-01-01 00:00:00' WHERE tmdb_id = 101")
        self.assertEqual(self.db.ingest_tmdb_title_details(tmdb.fetch, max_age_days=90), 1)
        self.assertEqual(tmdb.calls[-1], ("movie", 101))

    def test_not_found_is_recorded_and_not_returned(self):
        tmdb = FakeTMDB(missing={101})
        self.assertEqual(self.db.ingest_tmdb_title_details(tmdb.fetch), 2)
        self.assertEqual(self.db.ingest_tmdb_title_details(tmdb.fetch), 0)    # not retried until stale
        self.assertEqual(set(self.db.get_tmdb_title_details()["tmdb_id"]), {202})

    def test_partial_failure_keeps_going_and_total_failure_returns_minus_one(self):
        self.assertEqual(self.db.ingest_tmdb_title_details(FakeTMDB(broken={101}).fetch), 1)
        self.assertEqual(self.db.ingest_tmdb_title_details(FakeTMDB(broken={101}).fetch), -1)

    def test_limit(self):
        tmdb = FakeTMDB()
        self.assertEqual(self.db.ingest_tmdb_title_details(tmdb.fetch, limit=1), 1)
        self.assertEqual(self.db.ingest_tmdb_title_details(tmdb.fetch), 1)

    def test_unlinked_shows_are_found_from_one_episode_per_series(self):
        tmdb = FakeTMDB(finds={"tt-ep1": ("tv", 303)})
        self.assertEqual(self.db.ingest_tmdb_title_details(tmdb.fetch, tmdb.find), 3)
        self.assertEqual(tmdb.find_calls, ["tt-ep1"])
        self.assertIn(("tv", 303), tmdb.calls)
        # lookups are remembered, including misses
        self.db.ingest_tmdb_title_details(tmdb.fetch, tmdb.find)
        self.assertEqual(tmdb.find_calls, ["tt-ep1"])

    def test_emby_imdb_id_is_preferred_and_lists_are_decoded(self):
        tmdb = FakeTMDB(finds={"tt-ep1": ("tv", 303)})
        self.db.ingest_tmdb_title_details(tmdb.fetch, tmdb.find)
        d = self.db.get_tmdb_title_details().set_index("tmdb_id")
        self.assertEqual(d.loc[101, "emby_imdb_id"], "tt1")
        self.assertEqual(d.loc[101, "imdb_id"], "tt-tmdb-101")
        self.assertTrue(pd.isna(d.loc[303, "emby_imdb_id"]))    # only known to TMDB
        self.assertEqual(d.loc[101, "keywords"], ["k1"])
        self.assertEqual(d.loc[101, "people"], [{"id": 1, "name": "Someone", "role": "cast"}])


class TestIngestTMDBDiscoveryDetails(TMDBDetailsTestCase):

    def setUp(self):
        super().setUp()
        # library: movie 101, show 202. 500 is recommended for a library title; 600 only by 500, 700 only by a
        # popular title, so neither is fetched (the set would keep growing otherwise)
        self.tmdb = FakeTMDB(finds={"tt-pop": ("movie", 900), "tt1": ("movie", 101)},
                             recs={101: ["movie:500", "tv:202", "person:1"], 500: ["movie:600"], 900: ["movie:700"]})
        self.db.ingest_tmdb_title_details(self.tmdb.fetch)
        self.tmdb.calls.clear()

    def test_fetches_the_librarys_recommendations_and_popular_titles_only(self):
        popular = ["tt-pop", "tt1", "tt-none"]
        self.assertEqual(self.db.ingest_tmdb_discovery_details(self.tmdb.fetch, self.tmdb.find, popular), 2)
        self.assertEqual(sorted(self.tmdb.calls), [("movie", 500), ("movie", 900)])
        self.assertEqual(self.tmdb.find_calls, popular)
        # nothing due again, and lookups (misses too) are never repeated
        self.assertEqual(self.db.ingest_tmdb_discovery_details(self.tmdb.fetch, self.tmdb.find, popular), 0)
        self.assertEqual(len(self.tmdb.find_calls), 3)
        in_library = self.db.get_tmdb_title_details().set_index("tmdb_id")["in_library"]
        self.assertEqual(in_library.to_dict(), {101: True, 202: True, 500: False, 900: False})

    def test_limit_caps_lookups_and_fetches(self):
        self.assertEqual(self.db.ingest_tmdb_discovery_details(self.tmdb.fetch, self.tmdb.find, ["tt-pop", "tt1"], limit=1), 1)
        self.assertEqual(self.tmdb.find_calls, ["tt-pop"])

    def test_title_content_is_the_librarys_unless_asked(self):
        self.db.ingest_tmdb_discovery_details(self.tmdb.fetch, self.tmdb.find, ["tt-pop"])
        pp = PreProcess("test.db")
        try:
            library = pp.get_title_content()
            everything = pp.get_title_content(discovery=True).set_index("t_const")
        finally:
            pp.sqlite._connection.close()
        self.assertEqual(sorted(library["t_const"]), ["tt1", "tt2"])
        self.assertEqual(everything["in_library"].to_dict(),
                         {"tt-tmdb-500": False, "tt-tmdb-900": False, "tt1": True, "tt2": True})

    def test_titles_beyond_the_library_of_the_wrong_kind_are_left_out(self):
        # TMDB has junk "movie" entries claiming the IMDB ID of a series: their details describe something else
        self.db.ingest_tmdb_discovery_details(self.tmdb.fetch, self.tmdb.find, ["tt-pop"])
        imdb = PreProcess.encode_title_genres(pd.DataFrame([
            {"t_const": t, "primary_name": t, "type": kind, "start_year": "2001", "avg_rating": 7.0, "num_votes": 10,
             "genre": "Drama"} for t, kind in [("tt-tmdb-500", "tvSeries"), ("tt-tmdb-900", "movie"), ("tt1", "tvSeries")]
        ]))
        pp = PreProcess("test.db")
        try:
            c = pp.get_title_content(imdb, discovery=True)
        finally:
            pp.sqlite._connection.close()
        # 500 is a TMDB movie but an IMDB series; the library's own titles are kept whatever IMDB says
        self.assertEqual(sorted(c["t_const"]), ["tt-tmdb-900", "tt1", "tt2"])


class TestGetTitleContent(TMDBDetailsTestCase):

    def test_joins_imdb_metadata_and_falls_back_to_tmdb(self):
        tmdb = FakeTMDB(finds={"tt-ep1": ("tv", 303)})
        self.db.ingest_tmdb_title_details(tmdb.fetch, tmdb.find)
        imdb = PreProcess.encode_title_genres(pd.DataFrame([
            {"t_const": "tt1", "primary_name": "IMDB Name", "type": "movie", "start_year": "2001",
             "avg_rating": 7.5, "num_votes": 1234, "genre": g} for g in ("Crime", "Drama")
        ]))
        pp = PreProcess("test.db")
        try:
            c = pp.get_title_content(imdb).set_index("t_const")
        finally:
            pp.sqlite._connection.close()
        self.assertEqual(sorted(c.index), ["tt-tmdb-303", "tt1", "tt2"])
        self.assertEqual(c.loc["tt1", ["primary_name", "num_votes"]].tolist(), ["IMDB Name", 1234])
        self.assertEqual(c.loc["tt1", "genres"], ["Crime", "Drama"])
        # not in the IMDB data: TMDB's name, genres and type, no votes
        self.assertEqual(c.loc["tt2", ["primary_name", "type"]].tolist(), ["Title 202", "tvSeries"])
        self.assertEqual(c.loc["tt2", "genres"], ["Comedy"])
        self.assertTrue(pd.isna(c.loc["tt2", "num_votes"]))


def response(status, body=None, headers=None):
    r = mock.Mock(status_code=status, headers=headers or {})
    r.json.return_value = body
    r.raise_for_status.side_effect = requests.HTTPError(str(status)) if status >= 400 else None
    return r


class TestTMDBConnectorParsing(unittest.TestCase):

    def setUp(self):
        self.tmdb = TMDBConnector("token")
        self.tmdb._session = mock.Mock()

    def test_movie_people_roles_and_top_cast(self):
        cast = [{"id": i, "name": f"Actor {i}", "order": 20 - i} for i in range(20)]
        crew = [
            {"id": 900, "name": "Dir", "job": "Director"},
            {"id": 901, "name": "Writer", "job": "Screenplay"},
            {"id": 901, "name": "Writer", "job": "Writer"},     # same person, two writing credits
            {"id": 902, "name": "Grip", "job": "Key Grip"},
        ]
        self.tmdb._session.get.return_value = response(200, {
            "id": 5, "title": "M", "imdb_id": "tt5", "overview": "o", "original_language": "en",
            "genres": [{"name": "Action"}], "keywords": {"keywords": [{"name": "heist"}]},
            "belongs_to_collection": {"name": "M Collection"}, "credits": {"cast": cast, "crew": crew},
        })
        d = self.tmdb.fetch_title_details("movie", 5)
        roles = [(p["id"], p["role"]) for p in d["people"]]
        self.assertEqual(roles[:2], [(900, "director"), (901, "writer")])
        self.assertEqual([i for i, r in roles if r == "cast"], list(range(19, 11, -1)))     # 8 lowest "order"
        self.assertEqual((d["keywords"], d["collection"], d["imdb_id"]), (["heist"], "M Collection", "tt5"))

    def test_tv_uses_creators_aggregate_credits_and_external_ids(self):
        self.tmdb._session.get.return_value = response(200, {
            "id": 7, "name": "S", "overview": "", "genres": [], "keywords": {"results": [{"name": "sitcom"}]},
            "created_by": [{"id": 1, "name": "Creator"}], "external_ids": {"imdb_id": "tt7"},
            "aggregate_credits": {"cast": [{"id": 2, "name": "Lead", "order": 0}], "crew": []},
        })
        d = self.tmdb.fetch_title_details("tv", 7)
        self.assertEqual([(p["name"], p["role"]) for p in d["people"]], [("Creator", "creator"), ("Lead", "cast")])
        self.assertEqual((d["keywords"], d["imdb_id"], d["title"], d["collection"]), (["sitcom"], "tt7", "S", None))

    def test_404_is_none_and_rate_limit_is_retried(self):
        self.tmdb._session.get.return_value = response(404)
        self.assertIsNone(self.tmdb.fetch_title_details("movie", 1))
        self.tmdb._session.get.side_effect = [response(429, headers={"Retry-After": "0"}), response(200, {"movie_results": [{"id": 3}]})]
        self.assertEqual(self.tmdb.find_by_imdb_id("tt3"), ("movie", 3))

    def test_find_prefers_the_entry_with_the_most_votes(self):
        self.tmdb._session.get.return_value = response(200, {
            "movie_results": [{"id": 1798468, "vote_count": 0, "popularity": 0.1}],
            "tv_results": [{"id": 61888, "vote_count": 900, "popularity": 30.0}]})
        self.assertEqual(self.tmdb.find_by_imdb_id("tt3230454"), ("tv", 61888))

    def test_find_episode_resolves_to_its_show(self):
        self.tmdb._session.get.return_value = response(200, {"movie_results": [], "tv_results": [], "tv_episode_results": [{"id": 9, "show_id": 44}]})
        self.assertEqual(self.tmdb.find_by_imdb_id("tt-ep"), ("tv", 44))
        self.tmdb._session.get.return_value = response(200, {"movie_results": [], "tv_results": [], "tv_episode_results": []})
        self.assertIsNone(self.tmdb.find_by_imdb_id("tt-none"))

    def test_other_errors_raise(self):
        self.tmdb._session.get.return_value = response(401)
        with self.assertRaises(requests.HTTPError):
            self.tmdb.fetch_title_details("movie", 1)


if __name__ == "__main__":
    unittest.main()
