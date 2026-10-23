# Module: Machine Learning (ML)

The Machine Learning (ML) module provides classes for ML pre-processing, pipelines, and related functionality.

## **Class:** Pre-Process

```python
PreProcess(sqlite_db_name=None)  # defaults to SQLITE_DB_NAME from .env
```

Connects to the SQLite database straight away. The MySQL (IMDB) connection is only opened the first time an IMDB feature needs it, so the Emby-side methods work when the IMDB database host is offline (a failed connection is remembered, so it only costs one timeout).

### `map_emby_items_to_imdb(use_imdb_episodes=True)`

Maps every watched or library Emby item to the IMDB title it should be recommended/learned as - the **recommendation unit**. Movies and series map to themselves; episodes map to their parent series.

Episodes are resolved in order of confidence, recorded in `match_method`:

| `match_method` | Meaning |
| --- | --- |
| `self` | Movie/series in the library, using its own IMDB ID |
| `series_id` | Episode whose series is in `library_items` |
| `series_name` | Exact name match (case-insensitive, trailing `(year)` ignored) to a single current series. Covers episodes from before the Emby server rebuild (old series IDs no longer exist), and items only known from watch history, whose series name is parsed from `"<series> - s01e02 - <episode>"` |
| `imdb_episode` | Episode's own IMDB ID looked up in the IMDB `episodes` table (needs MySQL) |
| `unresolved_series` | Episode whose series couldn't be found |
| `no_imdb_id` | Resolved to a library item that has no IMDB provider ID |
| `not_in_library` | Movie deleted from the library before removed items were kept |
| `not_recommendable` | Not a movie/series/episode, e.g. live TV channels |

### `get_user_title_stats()`

Returns `(user_titles, unmatched)`. `user_titles` rolls `watch_hist_user_item_stats` up to one row per `(user_id, imdb_id)`: all episodes of a series, and the same title under old and new Emby IDs, are combined (summed sessions/seconds/outcomes, mean and best completion, `items_watched`, first/last watched). `unmatched` lists the user/item rows that couldn't be linked, with the reason - check it rather than assuming full coverage.

Note: user IDs also changed when the Emby server was rebuilt, so the same person can appear under two `user_id`s.

### `imdb_get_encoded_genres(cache_path, refresh=False)`

One-hot encodes IMDB title genres from MySQL, cached on disk.
