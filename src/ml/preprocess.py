import pandas as pd
import sys
from pathlib import Path
from connectors import MySQLConnector
from mysql.connector import Error as MySQLError

class PreProcess:
    """Helper class for pre-processing data for usage in models"""

    def __init__(self):
        pass
    
    def imdb_get_encoded_genres(self, cache_path: str = "data/cache/imdb_genres_ohe.parquet", refresh: bool = False) -> pd.DataFrame:
        """Fetch IMDB titles/genres, return one-hot encoded DF.
           Uses on-disk cache (.parquet or .pkl) unless refresh=True."""
        cache = Path(cache_path)

        # Load from cache if present (accept .parquet or .pkl)
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
            sql = MySQLConnector("scripts/mysql/.env")
            sql.curs.execute("""
                SELECT t.t_const, t.primary_name, g.genre
                FROM titles t
                JOIN genres g ON t.t_const = g.t_const
            """)
            rows = sql.curs.fetchall()
            cols = [d[0] for d in sql.curs.description] if sql.curs.description else ["t_const","primary_name","genre"]
            data = pd.DataFrame(rows, columns=cols)
            print("\nTitle data fetched!\n")
        except (Exception, MySQLError) as e:
            print(f"ERROR [imdb_get_encoded_genres]: Failed to fetch MySQL data: {e}", file=sys.stderr)
            sys.exit(1)
        
        if data.empty:
            raise Exception("ERROR [imdb_get_encoded_genres]: No data returned when fetching data")
        
        # One-hot encode only the 'genre' column
        dummies = pd.get_dummies(data["genre"], prefix="genre", dtype="uint8")
        encoded = pd.concat([data.drop(columns=["genre"]), dummies], axis=1)
        encoded = encoded.groupby(["t_const", "primary_name"], as_index=False).sum()

        # Use sparse types in-memory to save RAM
        for c in encoded.columns:
            if c.startswith("genre_"):
                encoded[c] = encoded[c].astype(pd.SparseDtype("uint8", 0))

        # Save cache. Prefer Parquet; if sparse not supported or no engine, fall back to pickle.
        cache.parent.mkdir(parents=True, exist_ok=True)
        try:
            # Densify only genre columns for Parquet
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