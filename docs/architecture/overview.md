# Architecture Overview

EMBRACE ties together a handful of focused connectors and storage layers to turn Emby usage data into recommendation-friendly features. The diagram below summarises the major components and their responsibilities.

```
Emby Server ──► EmbyConnector ─┬─► SQLiteConnector ─► SQLite (library + watch history)
                               │
                               ├─► TMDBConnector ─► TMDB API (genre lookups)
                               │
                               └─► PreProcess ──► MySQLConnector ─► MySQL (IMDB dataset)
```

## Core components

### EmbyConnector

- Authenticates using `BASE_DOMAIN` and `EMBY_API_KEY`.
- Paginates across `/Items` with `iter_all_items()` and optional library filters.
- Fetches full metadata for a specific item (`get_item_metadata`) so the SQLite ingest can capture fallback genres for episodes.
- Retrieves user watch history, either per user (`get_user_watch_hist`) or across the whole server (`get_all_watch_hist`).

### SQLiteConnector

- Creates and maintains the operational SQLite schema (`library_items`, `item_genres`, `item_tags`, `item_provider_ids`, watch-history tables, and TMDB reference tables).
- Handles bulk ingestion of library metadata, including pruning records that disappear from Emby.
- Provides watch-history processing helpers: raw event ingest, session aggregation, user-item statistics, and runtime-aware completion updates.
- Stores TMDB genre lookups so Emby genre names can be mapped back to canonical IDs.

### TMDBConnector

- Calls TMDB’s genre endpoints using the read access token you supply.
- Feeds `SQLiteConnector.ingest_tmdb_movie_tv_genres()` so the local database stays aligned with TMDB’s genre catalogue.

### MySQLConnector (optional)

- Loads credentials from `scripts/mysql/.env` (or an alternate path you provide).
- Establishes a connection to the dockerised MySQL instance that houses the IMDB dataset.
- Exposes a `cursor` and connection handle so scripts such as `imdb_create-schema.py`, `imdb_load-from-tsv.py`, and `PreProcess.imdb_get_encoded_genres()` can run parameterised SQL without reimplementing connection logic.

### PreProcess

- Contains feature-engineering routines used by downstream models.
- `imdb_get_encoded_genres()` queries the MySQL dataset, generates one-hot genre vectors, and saves a reusable cache on disk.
- Future preprocessing steps can reuse the same connector pattern to build embeddings or aggregate additional IMDB attributes.

## Typical data flow

1. **Sync reference data**
   - Fetch TMDB genre dictionaries with `TMDBConnector` and populate the SQLite tables.
2. **Ingest library metadata**
   - Use `EmbyConnector.iter_all_items()` as the source of truth for everything in your Emby library.
   - Pass the iterator (and optionally `get_item_metadata`) into `SQLiteConnector.ingest_all_library_items()` to update library tables and provider IDs.
3. **Process watch history**
   - Pull the full set of playback events with `EmbyConnector.get_all_watch_hist()`.
   - Run the watch-history pipeline (`_INIT_POPULATE_watch_hist_raw_events`, `_INIT_POPULATE_watch_hist_agg_sessions`, `_INIT_POPULATE_watch_hist_user_item_stats`, `update_completion_ratios`).
4. **Generate ML features**
   - Ensure the IMDB MySQL database is populated.
   - Call `PreProcess.imdb_get_encoded_genres()` to build or refresh the cached dataset used by the k-NN example in `src/main.py`.

Each connector is intentionally independent: you can run the SQLite pipeline without TMDB or the IMDB preprocessing step, and vice versa. This modularity keeps the system resilient when optional services are offline.
