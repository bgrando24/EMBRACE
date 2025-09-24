# Data Ingestion Workflows

This guide pulls together the recommended order of operations for keeping the local datasets aligned with Emby and TMDB. Every step is powered by the connector classes under `src/connectors/`.

## Prerequisites

- Populate `.env` with valid Emby and TMDB credentials.
- Install project dependencies (`pip install -e . -r requirements.txt`).
- Instantiate the connectors you plan to use:

  ```python
  from connectors import EmbyConnector, SQLiteConnector, TMDBConnector

  emby = EmbyConnector(debug=True)
  sqlite = SQLiteConnector(DB_NAME="EMBRACE_SQLITE_DB.db", debug=True)
  tmdb = TMDBConnector(TMDB_READ_ACCESS_TOKEN, debug=True)

  sqlite.connect_db()
  ```

## 1. Synchronise TMDB genres (optional but recommended)

1. Create the TMDB lookup tables if they do not already exist:

   ```python
   sqlite._INIT_create_tmdb_schemas()
   ```

2. Populate them with the latest TMDB responses:

   ```python
   sqlite.ingest_tmdb_movie_tv_genres(
       tmdb.fetch_movie_genres,
       tmdb.fetch_tv_genres,
   )
   ```

   When Emby returns genre names that TMDB does not recognise, `SQLiteConnector` generates stable hash IDs so the rows can still be joined.

## 2. Ingest library metadata

The goal is to populate `library_items`, `item_genres`, `item_tags`, and `item_provider_ids`.

```python
sqlite.ingest_all_library_items(
    emby_items_iterable=emby.iter_all_items(page_size=500),
    get_item_metadata=emby.get_item_metadata,  # optional but enables series-level fallback genres
)
```

- **Chunking**: `ingest_all_library_items` automatically batches inserts and periodically commits.
- **Pruning**: After ingestion completes, rows in SQLite that no longer exist in Emby are deleted (including child `item_*` rows).
- **Provider IDs**: TMDB, IMDB, and other provider IDs are normalised into `item_provider_ids` for easy joins with external datasets.

Run this step whenever your Emby library changes materially (new imports, deletions, metadata edits).

## 3. Import raw watch history

```python
sqlite._INIT_POPULATE_watch_hist_raw_events(
    emby_watch_hist_func=emby.get_all_watch_hist,
)
```

- The helper drops and recreates `watch_hist_raw_events` before inserting new data.
- Playback events prior to 15 August 2025 are shifted from PDT to Australia/Melbourne time to account for Emby’s historical reporting change.

If you only need incremental updates, consider wrapping `get_all_watch_hist` in a custom function that limits `num_days` before passing it to SQLite.

## 4. Aggregate sessions

```python
sqlite._INIT_POPULATE_watch_hist_agg_sessions(
    session_segment_minutes=15,
    completed_ratio_threshold=0.9,
    partial_ratio_threshold=0.25,
    min_sampled_seconds=60,
)
```

- **`session_segment_minutes`** controls how long a gap between events still counts as the same session.
- **Outcome thresholds**: tweak `completed_ratio_threshold`, `partial_ratio_threshold`, and `min_sampled_seconds` to better match your household’s viewing habits.
- All runtime-aware calculations fall back to reasonable defaults (25 minutes for episodes, 2 hours for movies) until `library_items` provides actual runtimes.

## 5. Build user-item statistics

```python
sqlite._INIT_POPULATE_watch_hist_user_item_stats()
```

This step recalculates totals, rewatches, adherence scores, and outcome counts per `(user_id, item_id)` pair. It is safe to rerun as often as you refresh the aggregated sessions.

## 6. Update completion ratios

```python
sqlite.update_completion_ratios()
```

- Replaces placeholder completion ratios with accurate values when `library_items.runtime_seconds` is available.
- Sessions without runtime metadata retain their fallback ratios.

## Automating the workflow

- **Order matters**: run the steps above in sequence so each layer has the data it expects.
- **Scheduling**: a nightly cron job or systemd timer works well. Combine library ingestion and watch-history processing in the same script to minimise API calls.
- **Error handling**: wrap connector calls in try/except blocks and log failures—Emby API hiccups or SQLite write issues should not crash the whole pipeline.

With the ingestion workflow in place, downstream analytics (including the k-NN demo in `src/main.py`) can rely on up-to-date library metadata, watch-history aggregates, and genre lookups.
