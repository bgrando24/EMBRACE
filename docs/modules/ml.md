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

Returns `(user_titles, unmatched)`. `user_titles` rolls `watch_hist_user_item_stats` up to one row per `(user_id, imdb_id)`: all episodes of a series, and the same title under old and new Emby IDs, are combined (summed sessions/seconds/outcomes, mean and best completion, `items_watched`, first/last watched, and `runtime_seconds` for movies). `unmatched` lists the user/item rows that couldn't be linked, with the reason - check it rather than assuming full coverage.

User IDs also changed when the Emby server was rebuilt, so the same person can appear under several `user_id`s. These are combined through the SQLite `user_aliases` table (alias user ID -> canonical user ID): `user_titles` is keyed by the canonical user and shows their current name. Manage aliases with:

```sh
PYTHONPATH=src python scripts/sqlite/set_user_alias.py --list                      # user IDs in the history + current aliases
PYTHONPATH=src python scripts/sqlite/set_user_alias.py <old id> <canonical id> --note "..."
```

### `imdb_get_encoded_genres(title_types=("movie", "tvSeries", "tvMiniSeries"), include_adult=False, cache_path=None, refresh=False)`

Fetches IMDB titles of the given types with their genres and ratings from MySQL and returns one row per title: `t_const`, `primary_name`, `type`, `start_year`, `avg_rating`, `num_votes`, then a 0/1 `genre_<name>` column per genre (multi-hot, via `MultiLabelBinarizer`, columns in alphabetical order). Titles with no genres are left out. Cached to `data/cache/imdb_genres_<types>.parquet`; pass `refresh=True` after re-importing IMDB.

The encoding step is `PreProcess.encode_title_genres(df)`, a static method usable on any `(title, genre)` rows.

### `get_library_imdb_ids()`

IMDB IDs of the movies and series currently on the Emby server - pass as `allowed` to only recommend what can be played.

## **Class:** GenreKNN

Item-to-item recommender ("because you watched X") over the multi-hot genre vectors, using cosine similarity.

```python
from ml import PreProcess, GenreKNN

pp = PreProcess()
knn = GenreKNN(pp.imdb_get_encoded_genres())
knn.recommend("tt4647692", n=10)                                   # anywhere on IMDB
knn.recommend("tt4647692", n=10, allowed=pp.get_library_imdb_ids()) # only titles on the Emby server
```

`recommend()` returns `t_const`, `primary_name`, `type`, `start_year`, `avg_rating`, `num_votes`, `similarity` (cosine, 0-1) and `shared_genres`. The seed is never returned; an unknown seed raises `KeyError`. `types=[...]` limits the IMDB title types returned.

**Ordering**: similarity, then `num_votes` (more popular first, missing = 0), then `t_const`. Genre vectors are coarse - IMDB gives at most 3 genres per title - so huge numbers of titles tie exactly (e.g. every `Comedy`-only title has similarity 1.0 to Letterkenny). The vote-count tie-break means that among identical genre sets, popular titles win; it's shown in the output so it isn't mistaken for genre similarity.

**Why not `sklearn.neighbors.NearestNeighbors`**: brute-force cosine kNN on L2-normalised rows is exactly one dot product per title, which `GenreKNN` computes directly (~50-130 ms over ~1M titles). `kneighbors(k)` returns an arbitrary k among exact ties, and can't filter to the Emby library before choosing k - the k nearest across all of IMDB may include none of your titles.

**Known limitation**: genre alone can't separate e.g. Letterkenny's dry humour from a broad comedy - Letterkenny and The Office get identical recommendations. This is the baseline that richer features (descriptions, people, style tags) should be measured against.

`profile_vector(t_consts, weights)` and `recommend_for_profile(profile, n, allowed, types, exclude)` do the same for a weighted blend of titles instead of one seed - see `UserGenreProfiles`. `recommend_for_profile()` returns `genres` (the title's own genres) instead of `shared_genres`.

## **Class:** UserGenreProfiles

Per-user recommendations from watch history ("based on everything you've watched"). Each user's taste profile is the interest-weighted sum of the genre vectors of what they watched; recommendations are the unwatched titles closest to it (cosine), ordered and tie-broken like `GenreKNN.recommend()`.

```python
from ml import PreProcess, GenreKNN, UserGenreProfiles

pp = PreProcess()
user_titles, _ = pp.get_user_title_stats()
profiles = UserGenreProfiles(GenreKNN(pp.imdb_get_encoded_genres()), user_titles)
profiles.users()                                                  # user_id, user_name, titles watched / in profile, hours
profiles.genre_shares(user_id)                                    # readable summary of the profile, e.g. Comedy 27%, Drama 18%
profiles.profile_titles(user_id)                                  # the titles that shape it, highest interest first
profiles.recommend(user_id, n=10, allowed=pp.get_library_imdb_ids())
```

**Interest** (`title_interest()`) - how much a title says about someone's taste, from how much of it they watched:

| Unit | Interest | Examples |
| --- | --- | --- |
| Movie | Fraction of the runtime watched, in total across all sessions, capped at 1 | Finished = 1, watched half = 0.5, rewatched = still 1 |
| Series | `log2(1 + hours / 2)` | 2 h = 1 (one finished movie), 6 h = 2, 14 h = 3, 135 h = 6.1 |

- Per-session completion isn't used: a movie watched over three evenings is three "partial" sessions, but was finished. Movies with no known runtime assume 2 hours.
- The log for series keeps one long binge from drowning out everything else, while still counting it the most.
- Titles under 0.1 (e.g. a movie opened for a few minutes) are left out of the profile, but like everything else the user has watched, they're never recommended back. There are no negative signals: abandoning one title says little about its genres.
- Titles the genre model doesn't cover (IMDB types other than movie/tvSeries/tvMiniSeries, e.g. `tvMovie`) are skipped; `users()` shows how many titles shape each profile.

**`because_you_watched`** on each recommendation is the profile title with the most similar genres, ties going to the higher-interest title (None if no genre is shared). It deliberately ignores interest otherwise, or a long binge would "explain" anything that shares one genre with it.

**Known limitation**: the same as `GenreKNN`, amplified. On the current library, many unwatched titles share the most common profile genres (e.g. `Action, Crime, Drama`), so the popularity tie-break picks the list and two users with similar profiles get the same recommendations. Profiles with less common genre mixes get more distinct results.

Demo: `PYTHONPATH=src python src/ml/main.py [IMDB ID ...]` - title-based recommendations for the given IDs, then per-user recommendations from the Emby library.
