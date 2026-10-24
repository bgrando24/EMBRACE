import numpy as np
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


    def get_title_content(self, imdb_titles: pd.DataFrame | None = None, movielens: pd.Series | None = None) -> pd.DataFrame:
        """
        One row per IMDB title with fetched TMDB details (see `SQLiteConnector.ingest_tmdb_title_details`), for
        `ContentKNN`.

        Args:
            imdb_titles (DataFrame, optional): output of `imdb_get_encoded_genres()`. Supplies IMDB names, types, years,
                ratings, vote counts (the tie-break) and genres; without it, or for titles it doesn't have, TMDB's are
                used and num_votes is missing.
            movielens (Series, optional): output of `get_movielens_likers()`, added as `movielens_likers`

        Returns:
            DataFrame: t_const, primary_name, type, start_year, avg_rating, num_votes, genres, keywords, collection,
                       original_language, people, overview, media_type, tmdb_id, recommendations
        """
        d = self.sqlite.get_tmdb_title_details()
        if d.empty:
            raise ValueError("No TMDB title details yet - run the refresh (or SQLiteConnector.ingest_tmdb_title_details)")
        # Emby's IMDB ID is what watch history is linked through, so it wins over TMDB's
        d["t_const"] = d["emby_imdb_id"].fillna(d["imdb_id"])
        d = d[d["t_const"].notna()].sort_values(["t_const", "media_type", "tmdb_id"]).drop_duplicates("t_const")

        out = pd.DataFrame({
            "t_const": d["t_const"],
            "primary_name": d["title"],
            "type": d["media_type"].map({"movie": "movie", "tv": "tvSeries"}),
            "start_year": pd.NA,
            "avg_rating": pd.NA,
            "num_votes": pd.NA,
            "genres": d["genres"],
        }).set_index("t_const")
        if imdb_titles is not None:
            imdb = imdb_titles.set_index("t_const")
            imdb = imdb.loc[imdb.index.intersection(out.index)]
            genre_cols = [c for c in imdb.columns if c.startswith("genre_")]
            for col in ["primary_name", "type", "start_year", "avg_rating", "num_votes"]:
                out.loc[imdb.index, col] = imdb[col]
            out.loc[imdb.index, "genres"] = pd.Series(
                [[c.removeprefix("genre_") for c in genre_cols if row[c]] for _, row in imdb[genre_cols].iterrows()],
                index=imdb.index, dtype=object,
            )

        out = out.reset_index()
        d = d.set_index("t_const").loc[out["t_const"]].reset_index()
        for col in ["keywords", "collection", "original_language", "people", "overview", "media_type", "tmdb_id", "recommendations"]:
            out[col] = d[col].to_numpy()
        if movielens is not None:
            out["movielens_likers"] = out["t_const"].map(movielens)
        out["num_votes"] = pd.to_numeric(out["num_votes"]).astype("Int64")
        out["avg_rating"] = pd.to_numeric(out["avg_rating"]).astype("Float64")
        return out


    def get_played_titles(self, user_titles: pd.DataFrame) -> pd.DataFrame:
        """
        Titles Emby has marked played for a user (`user_item_states`) that aren't in their watch history - watched
        before Playback Reporting's window, elsewhere, or marked by hand. Same columns as `get_user_title_stats()[0]`,
        so the two can be concatenated, with `source` = "emby_played" (history rows have no `source`).

        Watch time is estimated from the played items' runtimes (a played movie = watched once, a series = its played
        episodes). There are no dates: Emby's play counts and last-played dates were lost in the server rebuild.
        """
        states = self.sqlite.get_user_item_states()
        if states.empty:
            return user_titles.iloc[0:0].assign(source=pd.Series(dtype=object))
        states = states[states["played"] == 1].copy()
        aliases = self.sqlite.get_user_aliases().set_index("alias_user_id")["user_id"]
        states["user_id"] = states["user_id"].map(aliases).fillna(states["user_id"])

        mapping = self.map_emby_items_to_imdb(use_imdb_episodes=False).set_index("item_id")
        lib = self.sqlite.get_library_items().set_index("item_id")
        states = states.join(mapping[["imdb_id", "unit_type", "unit_name"]], on="item_id")
        # a played Series row only means "all episodes played", which the episode rows already say
        states = states[states["imdb_id"].notna() & (states["item_type"] != "Series")]
        states["seconds"] = states["item_id"].map(lib["runtime_seconds"]).fillna(0)

        seen = set(zip(user_titles["user_id"], user_titles["imdb_id"]))
        states = states[[(u, t) not in seen for u, t in zip(states["user_id"], states["imdb_id"])]]
        users = self.sqlite.get_watch_hist_users().set_index("user_id")["user_name"]
        played = (
            states.groupby(["user_id", "imdb_id", "unit_type"], as_index=False)
            .agg(unit_name=("unit_name", "first"), items_watched=("item_id", "nunique"),
                 total_seconds_watched=("seconds", "sum"))
        )
        # Emby's current name for users with no watch history (e.g. only played flags)
        emby_names = states.drop_duplicates("user_id").set_index("user_id")["user_name"]
        played["user_name"] = played["user_id"].map(users).fillna(played["user_id"].map(emby_names))
        movie_runtime = states[states["item_type"] == "Movie"].groupby(["user_id", "imdb_id"])["seconds"].max()
        played["runtime_seconds"] = [movie_runtime.get((u, t)) for u, t in zip(played["user_id"], played["imdb_id"])]
        played["source"] = "emby_played"
        return played


    MOVIELENS_DIR = "data/movielens/ml-32m"

    @staticmethod
    def get_movielens_likers(
        ml_dir: str = MOVIELENS_DIR,
        min_rating: float = 4.0,
        cache_path: str = "data/cache/movielens_likers.parquet",
        refresh: bool = False,
    ) -> pd.Series:
        """
        For every MovieLens movie, the MovieLens users who liked it (rated >= min_rating), keyed by IMDB ID - the
        input of ContentKNN's collaborative `movielens` block. Cached after the first run (reading the 32M ratings
        takes ~30 s).

        MovieLens 32M (GroupLens, University of Minnesota; https://grouplens.org/datasets/movielens/): 32M ratings of
        87k movies by 200k users, up to Oct 2023 - movies only, nothing newer. Free for non-commercial use; see
        docs/modules/ml.md for setup and licence.

        Returns:
            Series: t_const -> int32 array of MovieLens user IDs
        """
        cache = Path(cache_path)
        if cache.exists() and not refresh:
            df = pd.read_parquet(cache)
            return pd.Series(df["users"].to_numpy(), index=df["t_const"])

        links = pd.read_csv(Path(ml_dir) / "links.csv", usecols=["movieId", "imdbId"], dtype={"movieId": "int32", "imdbId": "int64"})
        links["t_const"] = "tt" + links["imdbId"].astype(str).str.zfill(7)
        ratings = pd.read_csv(Path(ml_dir) / "ratings.csv", usecols=["userId", "movieId", "rating"],
                              dtype={"userId": "int32", "movieId": "int32", "rating": "float32"})
        liked = ratings.loc[ratings["rating"] >= min_rating, ["userId", "movieId"]]
        liked = liked.sort_values("movieId", kind="stable")
        movie_ids, starts = np.unique(liked["movieId"].to_numpy(), return_index=True)
        users = pd.Series(np.split(liked["userId"].to_numpy(), starts[1:]), index=movie_ids)
        df = links.set_index("movieId").join(users.rename("users"), how="inner")[["t_const", "users"]]
        df = df.drop_duplicates("t_const").reset_index(drop=True)
        cache.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(cache, index=False, compression="zstd")
        return pd.Series(df["users"].to_numpy(), index=df["t_const"])


    def get_user_requests(self) -> pd.DataFrame:
        """
        Jellyseerr requests linked to watch-history users and IMDB titles.

        Requesters are matched by their linked Emby user ID, else by name (case-insensitive) against watch-history
        user names, then resolved through `user_aliases`. Titles are matched through TMDB details (so only titles that reached the
        library, or were looked up, resolve).

        Returns:
            DataFrame: user_id (canonical, NaN if the requester couldn't be matched), requester_name, t_const (NaN if
            unresolved), media_type, tmdb_id, status, media_status, requested_at (timestamp)
        """
        req = self.sqlite.get_jellyseerr_requests()
        if req.empty:
            return pd.DataFrame(columns=["user_id", "requester_name", "t_const", "media_type", "tmdb_id", "status",
                                         "media_status", "requested_at"])
        users = self.sqlite.get_watch_hist_users()
        by_name = users.assign(key=users["user_name"].str.casefold()).drop_duplicates("key").set_index("key")["user_id"]
        # the linked Emby user ID even without watch history yet (they may get one), else the name
        user_id = req["requester_emby_id"].fillna(req["requester_name"].str.casefold().map(by_name))
        aliases = self.sqlite.get_user_aliases().set_index("alias_user_id")["user_id"]
        req["user_id"] = user_id.map(aliases).fillna(user_id)

        d = self.sqlite.get_tmdb_title_details()
        if not d.empty:
            d["t_const"] = d["emby_imdb_id"].fillna(d["imdb_id"])
            t_const = d.drop_duplicates(["media_type", "tmdb_id"]).set_index(["media_type", "tmdb_id"])["t_const"]
            req["t_const"] = [t_const.get((m, t)) for m, t in zip(req["media_type"], req["tmdb_id"])]
        else:
            req["t_const"] = None
        req["requested_at"] = pd.to_datetime(req["requested_at"], utc=True, errors="coerce").dt.tz_localize(None)
        return req[["user_id", "requester_name", "t_const", "media_type", "tmdb_id", "status", "media_status", "requested_at"]]


    # interest a request counts for in a profile, when the requester hasn't watched the title (yet). Not used by
    # default: adding requests to profiles lowered both evaluations at 0.25-1.0 (see `ml.requests_signal.Requests`)
    REQUEST_INTEREST = 0.25

    def get_requested_titles(self, user_titles: pd.DataFrame, interest: float = REQUEST_INTEREST) -> pd.DataFrame:
        """
        Titles a user requested in Jellyseerr but hasn't watched, as rows like `get_user_title_stats()[0]` with
        `source` = "jellyseerr_request", sized so `title_interest()` gives `interest` (for a movie: that fraction of
        it watched; for a series the equivalent hours). Only titles with TMDB details (i.e. that reached the library).
        """
        req = self.get_user_requests().dropna(subset=["user_id", "t_const"])
        seen = set(zip(user_titles["user_id"], user_titles["imdb_id"]))
        req = req[[(u, t) not in seen for u, t in zip(req["user_id"], req["t_const"])]]
        req = req.sort_values("requested_at").drop_duplicates(["user_id", "t_const"])
        names = self.sqlite.get_watch_hist_users().set_index("user_id")["user_name"]
        titles = self.sqlite.get_tmdb_title_details()
        title_names = titles.assign(t=titles["emby_imdb_id"].fillna(titles["imdb_id"])).drop_duplicates("t").set_index("t")["title"]
        is_movie = req["media_type"] == "movie"
        # movie: interest = watched / runtime with a nominal 2h runtime; series: log2(1 + h / 2) = interest
        hours = np.where(is_movie, 2 * interest, 2 * (2 ** interest - 1))
        return pd.DataFrame({
            "user_id": req["user_id"].to_numpy(),
            "user_name": req["user_id"].map(names).fillna(req["requester_name"]).to_numpy(),
            "imdb_id": req["t_const"].to_numpy(),
            "unit_name": req["t_const"].map(title_names).to_numpy(),
            "unit_type": np.where(is_movie, "Movie", "Series"),
            "items_watched": 0,
            "total_seconds_watched": hours * 3600,
            "runtime_seconds": np.where(is_movie, 2 * 3600, np.nan),
            "requested_at": req["requested_at"].to_numpy(),
            "source": "jellyseerr_request",
        })


    def get_title_additions(self) -> pd.DataFrame:
        """
        When each library item was added to Emby, against the IMDB title it counts towards (episodes -> their series,
        see `map_emby_items_to_imdb()`), for `Freshness`.

        Returns:
            DataFrame: item_id, item_type, t_const, added_at (Emby DateCreated - NULL for items removed before it was stored)
        """
        lib = self.sqlite.get_library_items()
        lib = lib[lib["item_type"].isin(["Movie", "Series", "Episode"])]
        units = self.map_emby_items_to_imdb(use_imdb_episodes=False).set_index("item_id")["imdb_id"]
        return pd.DataFrame({
            "item_id": lib["item_id"],
            "item_type": lib["item_type"],
            "t_const": lib["item_id"].map(units),
            "added_at": pd.to_datetime(lib["date_created"], utc=True, errors="coerce"),
        }).reset_index(drop=True)


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
        under old and new Emby IDs combined). User IDs are resolved through `user_aliases`, so one person's history
        under several Emby user IDs is combined under their canonical user ID and current name.

        Returns:
            (user_titles, unmatched):
                user_titles - one row per (user_id, imdb_id) with summed sessions/time/outcomes, mean + best completion,
                              number of distinct Emby items watched (episodes for a series), first/last watched, and
                              `runtime_seconds` for movies (NaN for series, or when Emby doesn't know the runtime)
                unmatched   - the user/item stats rows that couldn't be linked to an IMDB title, with the reason
                              ('not_in_library' when the item was deleted before removed items were kept)
        """
        stats = self.sqlite.get_watch_hist_user_items_stats()
        users = self.sqlite.get_watch_hist_users()
        aliases = self.sqlite.get_user_aliases().set_index("alias_user_id")["user_id"]
        mapping = self.map_emby_items_to_imdb()

        stats["user_id"] = stats["user_id"].map(aliases).fillna(stats["user_id"])
        df = stats.merge(mapping, on="item_id", how="left").merge(users, on="user_id", how="left")
        df["match_method"] = df["match_method"].fillna("not_in_library")
        # per-session completion understates a movie watched over several sittings, so keep the runtime to compare
        # total time watched against
        runtimes = self.sqlite.get_library_items().set_index("item_id")["runtime_seconds"]
        df["runtime_seconds"] = df["item_id"].map(runtimes).where(df["item_type"] == "Movie")
        
        matched = df[df["imdb_id"].notna()]
        unmatched = df.loc[df["imdb_id"].isna(), ["user_id", "user_name", "item_id", "item_type", "match_method", "total_seconds_watched"]]
        
        user_titles = (
            # dropna=False: a missing user_name must not silently drop that user's rows
            matched.groupby(["user_id", "user_name", "imdb_id", "unit_type"], as_index=False, dropna=False)
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
                runtime_seconds=("runtime_seconds", "max"),
            )
        )
        return user_titles, unmatched
