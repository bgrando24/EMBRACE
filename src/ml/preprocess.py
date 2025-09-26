import pandas as pd
import sys
from pathlib import Path
from connectors import MySQLConnector, SQLiteConnector
from mysql.connector import Error as MySQLError
from dotenv import load_env
import os

class PreProcess:
    """Helper class for pre-processing data for usage in models"""

    def __init__(self):
        load_env()
        SQLITE_DB = os.getenv("SQLITE_DB_NAME) or "EMBRACE_SQLITE_DB.db"
        self.mysql = MySQLConnector("scripts/mysql/.env")
        self.sqlite = SQLiteConnector(SQLITE_DB, debug=True)
    
    def imdb_get_encoded_genres(self, cache_path: str = "data/cache/imdb_genres_ohe.parquet", refresh: bool = False) -> pd.DataFrame:
        """Fetch IMDB titles and their genres, return one-hot encoded pandas DataFrame
           Uses on-disk cache (.parquet or .pkl) unless refresh=True."""
        
        cache = Path(cache_path)

        # load from cache if present (accept .parquet or .pkl)
        if not refresh:
            candidates = []
            if cache.exists():
                candidates.append(cache)
            alt = cache.with_suffix(".pkl") if cache.suffix != ".pkl" else cache.with_suffix(".parquet")
            if alt.exists():
                candidates.append(alt)
            if candidates:
                chosen = candidates[0]
                print(f"Loading cached encoded genres from {chosen}")
                return pd.read_pickle(chosen) if chosen.suffix == ".pkl" else pd.read_parquet(chosen)

        try:
            print("\nAttempting to fetch title names and genres...\n")
            self.mysql.curs.execute("""
                SELECT t.t_const, t.primary_name, g.genre
                FROM titles t
                JOIN genres g ON t.t_const = g.t_const
            """)
            rows = self.mysql.curs.fetchall()
            cols = [d[0] for d in self.mysql.curs.description] if self.mysql.curs.description else ["t_const","primary_name","genre"]
            data = pd.DataFrame(rows, columns=cols)
            print("\nTitle data fetched!\n")
        except (Exception, MySQLError) as e:
            print(f"ERROR [imdb_get_encoded_genres]: Failed to fetch MySQL data: {e}", file=sys.stderr)
            sys.exit(1)
        
        if data.empty:
            raise Exception("ERROR [imdb_get_encoded_genres]: No data returned when fetching data")
        
        # one-hot encode only the 'genre' column
        dummies = pd.get_dummies(data["genre"], prefix="genre", dtype="uint8")
        encoded = pd.concat([data.drop(columns=["genre"]), dummies], axis=1)
        encoded = encoded.groupby(["t_const", "primary_name"], as_index=False).sum()

        # use sparse types in-memory to save RAM
        for c in encoded.columns:
            if c.startswith("genre_"):
                encoded[c] = encoded[c].astype(pd.SparseDtype("uint8", 0))

        # save to cache - prefer Parquet, if sparse not supported or no engine, fall back to pickle
        cache.parent.mkdir(parents=True, exist_ok=True)
        try:
            # densify only genre columns for Parquet
            dense = encoded.copy()
            genre_cols = [c for c in dense.columns if c.startswith("genre_")]
            for c in genre_cols:
                if isinstance(dense[c].dtype, pd.SparseDtype):
                    dense[c] = dense[c].sparse.to_dense().astype("uint8")
            dense.to_parquet(cache.with_suffix(".parquet"), index=False, compression="zstd")
            print(f"Saved encoded genres cache to {cache.with_suffix('.parquet')}")
        except Exception as e:
            pkl = cache.with_suffix(".pkl")
            encoded.to_pickle(pkl)
            print(f"Saved pickle cache to {pkl} (Parquet unavailable or unsupported: {e})")

        return encoded


    def match_emby_imdb_genres(self):
        """
        Matches up the provided Emby watch history DataFrame, with an IMDB record
        """
        
        # grab watch history, join on provider IDs
        watch_hist = self.sqlite.get_watch_hist_user_items_stats()
        providers = self.sqlite.get_item_provider_ids()
        # join pandas tables
