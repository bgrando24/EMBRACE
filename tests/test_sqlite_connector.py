import os
import sqlite3
import tempfile
import unittest

from connectors import SQLiteConnector


def event(date, time, item_id="100", duration="1500", user="u1"):
    return {
        "date": date, "time": time, "user_id": user, "item_name": f"Item {item_id}",
        "item_id": item_id, "item_type": "Episode", "duration": duration,
        "remote_address": "10.0.0.1", "user_name": user, "user_has_image": False,
    }


def library_item(item_id, genres=("Comedy",), providers=None):
    return {
        "Id": item_id, "Name": f"Item {item_id}", "Type": "Movie", "RunTimeTicks": 1500 * 10_000_000,
        "Genres": list(genres), "ProviderIds": providers or {"IMDB": f"tt{item_id}"},
    }


class SQLiteConnectorTestCase(unittest.TestCase):
    """Each test gets a fresh database in a temp dir (SQLiteConnector always writes under ./sqlite_db)"""

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

    def query(self, sql):
        return self.db._connection.execute(sql).fetchall()


class TestWatchHistIngest(SQLiteConnectorTestCase):

    def test_reingesting_same_history_adds_nothing(self):
        hist = {"u1": [event("2026-09-01", "20:00:00"), event("2026-09-02", "20:00:00")]}
        self.assertEqual(self.db.ingest_watch_hist_raw_events(lambda d, a: hist), 2)
        self.assertEqual(self.db.ingest_watch_hist_raw_events(lambda d, a: hist), 0)
        self.assertEqual(self.query("SELECT COUNT(*) FROM watch_hist_raw_events")[0][0], 2)

    def test_older_events_survive_when_source_window_moves_on(self):
        self.db.ingest_watch_hist_raw_events(lambda d, a: {"u1": [event("2025-09-01", "20:00:00")]})
        self.db.ingest_watch_hist_raw_events(lambda d, a: {"u1": [event("2026-10-01", "20:00:00")]})
        self.assertEqual(self.query("SELECT COUNT(*) FROM watch_hist_raw_events")[0][0], 2)

    def test_stub_and_real_play_with_same_timestamp_are_both_kept(self):
        # seen in real data: a 0s row and the full play logged at the same second
        hist = {"u1": [event("2025-07-19", "18:59:12", duration="0"), event("2025-07-19", "18:59:12", duration="7870")]}
        self.assertEqual(self.db.ingest_watch_hist_raw_events(lambda d, a: hist), 2)

    def test_pre_cutoff_timestamps_shift_once(self):
        hist = {"u1": [event("2025-08-14", "17:23:18"), event("2025-08-15", "11:10:00")]}
        self.db.ingest_watch_hist_raw_events(lambda d, a: hist)
        self.db.ingest_watch_hist_raw_events(lambda d, a: hist)
        rows = self.query("SELECT date || ' ' || time FROM watch_hist_raw_events ORDER BY 1")
        self.assertEqual([r[0] for r in rows], ["2025-08-15 10:23:18", "2025-08-15 11:10:00"])

    def test_unique_index_added_to_existing_table(self):
        # databases created before the index existed
        self.db._connection.execute("""CREATE TABLE watch_hist_raw_events(
            row_id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT NOT NULL, time TEXT NOT NULL, user_id TEXT NOT NULL,
            item_name TEXT NOT NULL, item_id TEXT NOT NULL, item_type TEXT NOT NULL, duration INTEGER NOT NULL,
            remote_address TEXT, user_name TEXT NOT NULL)""")
        hist = {"u1": [event("2026-09-01", "20:00:00")]}
        self.db.ingest_watch_hist_raw_events(lambda d, a: hist)
        self.assertEqual(self.db.ingest_watch_hist_raw_events(lambda d, a: hist), 0)


class TestLibraryIngest(SQLiteConnectorTestCase):

    def test_reingest_does_not_duplicate_child_rows(self):
        items = [library_item("1", genres=("Comedy", "Crime"))]
        self.db.ingest_all_library_items(iter(items))
        self.db.ingest_all_library_items(iter(items))
        self.assertEqual(self.query("SELECT COUNT(*) FROM item_genres")[0][0], 2)
        self.assertEqual(self.query("SELECT COUNT(*) FROM item_provider_ids")[0][0], 1)

    def test_provider_names_stored_lowercase(self):
        self.db.ingest_all_library_items(iter([library_item("1", providers={"IMDB": "tt1"}), library_item("2", providers={"Imdb": "tt2"})]))
        self.assertEqual(self.query("SELECT DISTINCT provider FROM item_provider_ids"), [("imdb",)])

    def test_existing_mixed_case_providers_are_migrated(self):
        self.db.ingest_all_library_items(iter([library_item("1")]))
        self.db._connection.execute("INSERT INTO item_provider_ids VALUES ('old', 'Imdb', 'tt9')")
        self.db._connection.commit()
        self.db.ingest_all_library_items(iter([library_item("1")]))
        self.assertEqual(self.query("SELECT provider FROM item_provider_ids WHERE item_id = 'old'"), [("imdb",)])

    def test_missing_items_are_flagged_not_deleted(self):
        self.db.ingest_all_library_items(iter([library_item("1"), library_item("2")]))
        self.db.ingest_all_library_items(iter([library_item("1")]))
        rows = self.query("SELECT item_id, removed_at IS NOT NULL FROM library_items ORDER BY item_id")
        self.assertEqual(rows, [("1", 0), ("2", 1)])
        self.assertEqual(self.query("SELECT COUNT(*) FROM item_genres WHERE item_id = '2'")[0][0], 1)

    def test_item_returning_to_library_is_unflagged(self):
        self.db.ingest_all_library_items(iter([library_item("1"), library_item("2")]))
        self.db.ingest_all_library_items(iter([library_item("1")]))
        self.db.ingest_all_library_items(iter([library_item("1"), library_item("2")]))
        self.assertEqual(self.query("SELECT COUNT(*) FROM library_items WHERE removed_at IS NOT NULL")[0][0], 0)

    def test_empty_fetch_does_not_flag_library(self):
        self.db.ingest_all_library_items(iter([library_item("1")]))
        self.assertFalse(self.db.ingest_all_library_items(iter([])))
        self.assertEqual(self.query("SELECT COUNT(*) FROM library_items WHERE removed_at IS NOT NULL")[0][0], 0)


class TestStatsFetch(SQLiteConnectorTestCase):

    def test_user_item_stats_dataframe(self):
        self.db.ingest_all_library_items(iter([library_item("100")]))
        self.db.ingest_watch_hist_raw_events(lambda d, a: {"u1": [event("2026-09-01", "20:00:00")]})
        self.db._INIT_POPULATE_watch_hist_agg_sessions()
        self.db._INIT_POPULATE_watch_hist_user_item_stats()
        df = self.db.get_watch_hist_user_items_stats()
        self.assertEqual(len(df), 1)
        self.assertEqual(df.loc[0, "total_seconds_watched"], 1500)


if __name__ == "__main__":
    unittest.main()
