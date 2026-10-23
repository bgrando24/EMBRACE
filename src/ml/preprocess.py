import pandas as pd
import re
import sys
from pathlib import Path
from connectors import MySQLConnector, SQLiteConnector
from mysql.connector import Error as MySQLError
from sklearn.preprocessing import MultiLabelBinarizer
from dotenv import load_dotenv
import os

class PreProcess:
    """Helper class for pre-processing data for usage in models"""

    def __init__(self, sqlite_db_name: str | None = None):
        load_dotenv()
        SQLITE_DB = sqlite_db_name or os.getenv("SQLITE_DB_NAME") or "EMBRACE_SQLITE_DB.db"
        self.sqlite = SQLiteConnector(SQLITE_DB, debug=True)
        self.sqlite.connect_db()
        self._mysql: MySQLConnector | None = None
        self._mysql_error: Exception | None = None

    @property
    def mysql(self) -> MySQLConnector:
        """MySQL (IMDB) connection, only opened when first needed so Emby-only features work without it.
           A failed connection is remembered, so an offline database host doesn't cost a timeout on every call."""
        if self._mysql_error is not None:
            raise ConnectionError(f"MySQL unavailable (earlier attempt failed): {self._mysql_error}")
        if self._mysql is None:
            try:
                self._mysql = MySQLConnector("scripts/mysql/.env")
            except Exception as e:
                self._mysql_error = e
                raise
        return self._mysql
    
    # IMDB title types we recommend: movies and shows, not episodes, shorts, video games etc.
    DEFAULT_TITLE_TYPES = ("movie", "tvSeries", "tvMiniSeries")
    
    def imdb_get_encoded_genres(
        self,
        title_types: tuple[str, ...] = DEFAULT_TITLE_TYPES,
        include_adult: bool = False,
        cache_path: str | None = None,
        refresh: bool = False,
    ) -> pd.DataFrame:
        """
        Fetch IMDB titles of the given types with their genres and ratings, return a multi-hot encoded DataFrame
        (see `encode_title_genres`). Uses an on-disk Parquet cache unless refresh=True.
        
        Args:
            title_types (tuple[str], optional): IMDB title types to include - DEFAULT: movie, tvSeries, tvMiniSeries
            include_adult (bool, optional): Include titles IMDB flags as adult - DEFAULT: False
            cache_path (str, optional): DEFAULT: data/cache/imdb_genres_<title types>[_adult].parquet
            refresh (bool, optional): Ignore the cache and re-query MySQL
        """
        suffix = "_adult" if include_adult else ""
        cache = Path(cache_path or f"data/cache/imdb_genres_{'-'.join(sorted(title_types))}{suffix}.parquet")
        if cache.exists() and not refresh:
            print(f"Loading cached encoded genres from {cache}")
            return pd.read_parquet(cache)

        try:
            print("\nAttempting to fetch title names and genres...\n")
            placeholders = ",".join(["%s"] * len(title_types))
            self.mysql.curs.execute(f"""
                SELECT t.t_const, t.primary_name, t.type, t.start_year, r.avg_rating, r.num_votes, g.genre
                FROM titles t
                JOIN genres g ON t.t_const = g.t_const
                LEFT JOIN ratings r ON t.t_const = r.t_const
                WHERE t.type IN ({placeholders})
                  {"" if include_adult else "AND (t.is_adult = 0 OR t.is_adult IS NULL)"}
            """, list(title_types))
            cols = [d[0] for d in self.mysql.curs.description]
            data = pd.DataFrame(self.mysql.curs.fetchall(), columns=cols)
            print("\nTitle data fetched!\n")
        except (Exception, MySQLError) as e:
            print(f"ERROR [imdb_get_encoded_genres]: Failed to fetch MySQL data: {e}", file=sys.stderr)
            raise
        
        if data.empty:
            raise Exception("ERROR [imdb_get_encoded_genres]: No data returned when fetching data")
        
        encoded = self.encode_title_genres(data)
        cache.parent.mkdir(parents=True, exist_ok=True)
        encoded.to_parquet(cache, index=False, compression="zstd")
        print(f"Saved encoded genres cache to {cache}")
        return encoded


    @staticmethod
    def encode_title_genres(data: pd.DataFrame) -> pd.DataFrame:
        """
        Turns one row per (title, genre) into one row per title with a multi-hot `genre_<name>` column per genre.
        
        Args:
            data (DataFrame): t_const, primary_name, type, start_year, avg_rating, num_votes, genre
        
        Returns:
            DataFrame: one row per t_const, the non-genre columns, then uint8 genre_* columns in alphabetical order
        """
        meta = data.drop(columns="genre").drop_duplicates("t_const").set_index("t_const")
        genre_lists = data.groupby("t_const")["genre"].agg(lambda g: sorted(set(g)))
        
        # MultiLabelBinarizer expects one collection of labels per sample; classes_ (the vocabulary) are sorted
        mlb = MultiLabelBinarizer()
        X = mlb.fit_transform(genre_lists)
        genres = pd.DataFrame(X.astype("uint8"), columns=[f"genre_{g}" for g in mlb.classes_], index=genre_lists.index)
        
        encoded = meta.loc[genre_lists.index].join(genres).reset_index()
        encoded["num_votes"] = encoded["num_votes"].astype("Int64")
        encoded["avg_rating"] = encoded["avg_rating"].astype("Float64")
        return encoded


    def get_library_imdb_ids(self) -> set[str]:
        """IMDB IDs of the movies and series currently on the Emby server"""
        lib = self.sqlite.get_library_items()
        current = lib.loc[lib["removed_at"].isna() & lib["item_type"].isin(["Movie", "Series"]), "item_id"]
        prov = self.sqlite.get_item_provider_ids()
        return set(prov.loc[(prov["provider"] == "imdb") & prov["item_id"].isin(current), "provider_item_id"])


    def map_emby_items_to_imdb(self, use_imdb_episodes: bool = True) -> pd.DataFrame:
        """
        Maps every watched or library Emby item to the IMDB title it should be recommended/learned as.
        
        Movies and series map to themselves; episodes map to their parent series, since we recommend shows, not episodes.
        Episodes are resolved to a series in order of confidence:
            1. 'series_id'    - the episode's series is in library_items
            2. 'series_name'  - exact (case-insensitive, ignoring a trailing "(year)") name match to a single current series.
                                Needed because Emby item IDs changed when the server was rebuilt, so older episodes point at
                                series IDs that no longer exist. For items only known from watch history, the series name
                                is parsed from the event's item name ("<series> - s01e02 - <episode>").
            3. 'imdb_episode' - the episode's own IMDB ID looked up in the IMDB `episodes` table (MySQL, optional - skipped
                                with a warning if the database is unavailable)
        
        Args:
            use_imdb_episodes (bool, optional): Whether to try step 3 - DEFAULT: True
        
        Returns:
            DataFrame: item_id, item_type, in_library, unit_item_id, unit_type ('Movie' | 'Series'), unit_name, imdb_id, match_method
            
            match_method is one of: 'self', 'series_id', 'series_name', 'imdb_episode', 'unresolved_series', 'no_imdb_id',
            'not_in_library' (a movie deleted before removed items were kept), 'not_recommendable' (e.g. live TV channels)
        """
        lib = self.sqlite.get_library_items()
        prov = self.sqlite.get_item_provider_ids()
        imdb_ids = prov.loc[prov["provider"] == "imdb"].set_index("item_id")["provider_item_id"]
        
        # items only known from watch history: deleted from library_items before removed items were kept
        hist_items = self.sqlite.get_watch_hist_items()
        orphans = hist_items[~hist_items["item_id"].isin(lib["item_id"])].copy()
        orphans["series_name"] = orphans["item_name"].str.extract(r"^(.*?) - s\d+e\d+", flags=re.IGNORECASE)[0]
        orphans["series_id"] = None
        
        items = pd.concat(
            [lib.assign(in_library=True), orphans.assign(in_library=False)],
            ignore_index=True,
        )[["item_id", "item_type", "item_name", "series_id", "series_name", "removed_at", "in_library"]]
        
        def name_key(names: pd.Series) -> pd.Series:
            return names.fillna("").str.replace(r"\s*\(\d{4}\)$", "", regex=True).str.casefold()
        
        series = lib[lib["item_type"] == "Series"]
        # only use names that identify exactly one current series, otherwise the match is ambiguous
        current_series = series[series["removed_at"].isna()]
        keys = name_key(current_series["item_name"])
        unique = ~keys.duplicated(keep=False)
        series_by_name = dict(zip(keys[unique], current_series.loc[unique, "item_id"]))
        
        out = items[["item_id", "item_type", "in_library"]].copy()
        out["unit_item_id"] = None
        out["unit_type"] = None
        out["imdb_id"] = None
        out["unit_name"] = None
        out["match_method"] = "not_recommendable"
        
        is_title = items["item_type"].isin(["Movie", "Series"])
        own = is_title & items["in_library"]
        out.loc[own, "unit_item_id"] = items.loc[own, "item_id"]
        out.loc[own, "unit_type"] = items.loc[own, "item_type"]
        out.loc[own, "match_method"] = "self"
        out.loc[is_title & ~items["in_library"], "match_method"] = "not_in_library"
        
        is_episode = items["item_type"] == "Episode"
        out.loc[is_episode, "unit_type"] = "Series"
        
        via_id = is_episode & items["series_id"].isin(set(series["item_id"]))
        out.loc[via_id, "unit_item_id"] = items.loc[via_id, "series_id"]
        out.loc[via_id, "match_method"] = "series_id"
        
        remaining = is_episode & ~via_id
        by_name = name_key(items.loc[remaining, "series_name"]).map(series_by_name)
        out.loc[remaining, "unit_item_id"] = by_name
        out.loc[remaining, "match_method"] = by_name.notna().map({True: "series_name", False: "unresolved_series"})
        
        # IMDB ID + display name of the resolved library unit
        has_unit = out["unit_item_id"].notna()
        out.loc[has_unit, "imdb_id"] = out.loc[has_unit, "unit_item_id"].map(imdb_ids)
        out.loc[has_unit, "unit_name"] = out.loc[has_unit, "unit_item_id"].map(lib.set_index("item_id")["item_name"])
        
        unresolved = out["match_method"] == "unresolved_series"
        if use_imdb_episodes and unresolved.any():
            episode_imdb = out.loc[unresolved, "item_id"].map(imdb_ids).dropna()
            parents = self.imdb_get_episode_parents(list(episode_imdb.unique()))
            if parents is not None and not parents.empty:
                parent_ids = episode_imdb.map(parents.set_index("t_const")["parent_t_const"]).dropna()
                parent_names = parent_ids.map(parents.drop_duplicates("parent_t_const").set_index("parent_t_const")["parent_name"])
                out.loc[parent_ids.index, "imdb_id"] = parent_ids
                out.loc[parent_ids.index, "unit_name"] = parent_names.fillna(items.loc[parent_ids.index, "series_name"])
                out.loc[parent_ids.index, "match_method"] = "imdb_episode"
        
        resolved = out["match_method"].isin(["self", "series_id", "series_name"])
        out.loc[resolved & out["imdb_id"].isna(), "match_method"] = "no_imdb_id"
        return out


    def imdb_get_episode_parents(self, episode_t_consts: list[str]) -> pd.DataFrame | None:
        """
        Looks up the parent series of IMDB episode IDs in the MySQL `episodes` table.
        
        Returns:
            DataFrame (t_const, parent_t_const, parent_name), or None if the IMDB database is unavailable
        """
        if not episode_t_consts:
            return pd.DataFrame(columns=["t_const", "parent_t_const", "parent_name"])
        try:
            placeholders = ",".join(["%s"] * len(episode_t_consts))
            self.mysql.curs.execute(
                f"""SELECT e.t_const, e.parent_t_const, t.primary_name AS parent_name
                    FROM episodes e LEFT JOIN titles t ON t.t_const = e.parent_t_const
                    WHERE e.t_const IN ({placeholders})""",
                episode_t_consts,
            )
            return pd.DataFrame(self.mysql.curs.fetchall(), columns=["t_const", "parent_t_const", "parent_name"])
        except (ConnectionError, MySQLError, FileNotFoundError, RuntimeError) as e:
            print(f"WARN [imdb_get_episode_parents]: IMDB database unavailable, skipping episode -> series lookup: {e}", file=sys.stderr)
            return None


    def get_user_title_stats(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Rolls the per user/item watch stats up to per user/IMDB title (episodes -> their series, and the same title
        under old and new Emby IDs combined).
        
        Returns:
            (user_titles, unmatched):
                user_titles - one row per (user_id, imdb_id) with summed sessions/time/outcomes, mean + best completion,
                              number of distinct Emby items watched (episodes for a series), first/last watched
                unmatched   - the user/item stats rows that couldn't be linked to an IMDB title, with the reason
                              ('not_in_library' when the item was deleted before removed items were kept)
        """
        stats = self.sqlite.get_watch_hist_user_items_stats()
        users = self.sqlite.get_watch_hist_users()
        mapping = self.map_emby_items_to_imdb()
        
        df = stats.merge(mapping, on="item_id", how="left").merge(users, on="user_id", how="left")
        df["match_method"] = df["match_method"].fillna("not_in_library")
        
        matched = df[df["imdb_id"].notna()]
        unmatched = df.loc[df["imdb_id"].isna(), ["user_id", "user_name", "item_id", "item_type", "match_method", "total_seconds_watched"]]
        
        user_titles = (
            matched.groupby(["user_id", "user_name", "imdb_id", "unit_type"], as_index=False)
            .agg(
                unit_name=("unit_name", "first"),
                items_watched=("item_id", "nunique"),
                total_sessions=("total_sessions", "sum"),
                total_seconds_watched=("total_seconds_watched", "sum"),
                completed_sessions=("completed_sessions", "sum"),
                partial_sessions=("partial_sessions", "sum"),
                sampled_sessions=("sampled_sessions", "sum"),
                abandoned_sessions=("abandoned_sessions", "sum"),
                mean_completion_ratio=("average_completion_ratio", "mean"),
                best_completion_ratio=("best_completion_ratio", "max"),
                first_watched_timestamp=("first_watched_timestamp", "min"),
                last_watched_timestamp=("last_watched_timestamp", "max"),
            )
        )
        return user_titles, unmatched
