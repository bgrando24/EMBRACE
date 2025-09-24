# SQLite Database Reference

The project stores its operational state in a SQLite database that lives under `sqlite_db/`. All schemas are created and maintained by `SQLiteConnector`; no manual migrations are required. The connector will automatically create the database directory, initialize the tables, and keep library metadata in sync as Emby changes over time.

## Library metadata tables

### `library_items`

Central catalogue of every Movie and Episode imported from Emby. Each row contains:

- **Identity**: `item_id` (primary key), `item_type`, `item_name`.
- **Series context**: `series_id`, `series_name`, `season_number`, `episode_number` for episodic content.
- **Runtime and chronology**: `runtime_ticks` (with generated seconds/minutes columns), `premiere_date`, and `date_created`.
- **Descriptive metadata**: `overview`, `community_rating`, `production_year`.
- **File attributes**: `file_path`, `container`, `video_codec`, `resolution_width`, `resolution_height`.
- **Timestamps**: `last_updated` auto-populates whenever the row is refreshed.

Indexes (`idx_library_series`, `idx_library_type`, `idx_series_season_ep`, `idx_library_name`, `idx_library_year`) keep lookups responsive for common filtering patterns.

### `item_genres`

A many-to-many bridge that stores the genre tags attached to each library item. Genres sourced directly from Emby retain their numeric IDs; otherwise a deterministic CRC32-derived ID is generated so joins remain stable across runs.

### `item_tags`

Stores free-form tag labels (e.g., “Kids”, “4K”) associated with a library item. Tags can originate from Emby’s `TagItems` collection or the fallback `Tags` array. A composite primary key (`item_id`, `tag_id`) prevents duplicates.

### `item_provider_ids`

Normalises the `ProviderIds` payload emitted by Emby. Each row captures a provider namespace (TMDB, IMDB, TVDB, etc.) alongside the provider-specific identifier so you can map items back to external datasets. Records are refreshed on every ingest, and rows belonging to media removed from Emby are pruned automatically.

## TMDB reference tables

`SQLiteConnector._INIT_create_tmdb_schemas()` provisions two lookup tables:

- `tmdb_movie_genres`
- `tmdb_tv_genres`

Populate them with `SQLiteConnector.ingest_tmdb_movie_tv_genres(...)` so `item_genres` can map Emby names to canonical TMDB IDs. When a genre label is missing from the TMDB response, the connector falls back to the deterministic hash strategy described above.

## Watch history tables

The watch-history pipeline converts raw Emby playback events into progressively richer aggregates.

### `watch_hist_raw_events`

Snapshot of Emby’s playback reporting API. Each row stores the recorded `date` and `time` (normalised to Australia/Melbourne time for pre-August 2025 entries), the `user_id`, `user_name`, `item_id`, `item_name`, `item_type`, playback `duration`, and optional `remote_address` metadata.

### `watch_hist_agg_sessions`

Aggregates raw events into contiguous sessions. Key fields include:

- `session_start_timestamp` / `session_end_timestamp`
- `session_span_minutes` and `total_seconds_watched`
- `session_count` (number of underlying raw events)
- `completion_ratio` (capped at 1.0 once runtime metadata is available)
- `outcome` classification (`sampled`, `abandoned`, `partial`, `completed`)

Uniqueness is enforced per `(user_id, item_id, session_start_timestamp)` and each row links back to `library_items` via `item_id`.

### `watch_hist_user_item_stats`

Derived statistics summarising a user’s relationship with a specific item. Generated columns (e.g., `total_minutes_watched`, `days_between_first_last`) and counters (`completed_sessions`, `rewatch_count`, `adherence_score`) are recalculated every time `_INIT_POPULATE_watch_hist_user_item_stats()` runs. The table is keyed by `user_id` + `item_id` and also links to `library_items` for runtime-aware metrics.

## Watch history processing pipeline

Run the following steps—typically in this order—to refresh watch history:

1. **Library ingest** (`ingest_all_library_items`): Populate `library_items`, `item_genres`, `item_tags`, and `item_provider_ids`. Provide `EmbyConnector.iter_all_items()` and optionally `EmbyConnector.get_item_metadata()` so the connector can fetch series-level genres for episodes.
2. **Raw event sync** (`_INIT_POPULATE_watch_hist_raw_events`): Downloads the full playback history via `EmbyConnector.get_all_watch_hist()`, normalises timestamps, and bulk-loads the `watch_hist_raw_events` table.
3. **Session aggregation** (`_INIT_POPULATE_watch_hist_agg_sessions`): Groups raw events into sessions. Tunable parameters include `session_segment_minutes`, completion thresholds, and minimum seconds for the `sampled` outcome.
4. **User-item statistics** (`_INIT_POPULATE_watch_hist_user_item_stats`): Summarises aggregate engagement for each `(user, item)` pair, calculating adherence scores and outcome counts.
5. **Runtime-aware completion update** (`update_completion_ratios`): Rewrites `completion_ratio` for sessions when actual runtime data is available in `library_items`.

Running the steps nightly (or after large library changes) keeps the downstream analytics model aligned with the latest viewing behaviour.

## Index summary

Besides the library indexes mentioned earlier, the connector maintains:

- `idx_watch_hist_raw_user_time` on `watch_hist_raw_events(user_id, date, time DESC)`
- `idx_watch_hist_agg_sessions` on `watch_hist_agg_sessions(user_id, session_end_timestamp DESC)`
- `idx_watch_hist_agg_item` on `watch_hist_agg_sessions(item_id)`
- `idx_watch_hist_user_item_stats` on `watch_hist_user_item_stats(user_id, adherence_score DESC)`
- `idx_genres_item` on `item_genres(item_id)`
- `idx_tags_item` on `item_tags(item_id)`
- `idx_provider_provider` on `item_provider_ids(provider)`

These indexes are recreated automatically with `CREATE INDEX IF NOT EXISTS`, so repeated pipeline runs are safe.
