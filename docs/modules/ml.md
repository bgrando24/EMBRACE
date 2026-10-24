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

`GenreKNN` and `ContentKNN` share their ranking code through the base class `CosineKNN`, which also provides `profile_vector(t_consts, weights)` and `recommend_for_profile(profile, n, allowed, types, exclude)`: the same ranking for a weighted blend of titles instead of one seed (see `UserProfiles`). `recommend_for_profile()` returns `genres` (the title's own genres) instead of the shared column.

## **Class:** ContentKNN

Recommender over richer content than genre: TMDB keywords, people and descriptions. It only covers titles with fetched TMDB details: the library's movies and series, including removed ones still in watch history. Scoring is the same cosine similarity, ordering and tie-break as `GenreKNN`.

```python
from ml import PreProcess, ContentKNN

pp = PreProcess()
knn = ContentKNN(pp.get_title_content(pp.imdb_get_encoded_genres()))
knn.recommend("tt3890160", n=10, allowed=pp.get_library_imdb_ids())   # Baby Driver -> The Town, Wrath of Man, Den of Thieves
```

`recommend()` returns `shared`: what each recommendation has in common with the seed. Shared keywords and people come first, ordered by how much they add to the similarity, then shared genres. Example: `[Jon Hamm, bank robbery, heist, shootout, Crime]`.

| Block | Features | Default weight |
| --- | --- | --- |
| `genre` | IMDB genres (TMDB's if IMDB has none) | 0.05 |
| `keywords` | TMDB keywords, `collection: <franchise>` for movies, `language: <code>` when not English | 0.4 |
| `people` | Director + writers (movies) or creators (series), and the top 8 billed cast, by TMDB person ID | 0.35 |
| `description` | Words of the TMDB overview, English stop words removed | 0.2 |

How the features are scored and combined:
- **TF-IDF weighting:** each block is TF-IDF weighted and L2-normalised on its own. A shared keyword or actor counts for more the rarer it is, so "workplace comedy" outweighs "based on novel or book".
- **Single-title tokens dropped:** a token found in only one title can never match anything, so it is left out.
- **Combining blocks:** the blocks are concatenated with `sqrt(weight)` scaling. Similarity is therefore the weighted sum of the per-block cosines, re-normalised over the blocks a title has.

**Weights** are `ContentKNN.DEFAULT_WEIGHTS`, chosen with `ml.evaluate` (below). Every one of the 10 best grid weightings gave genre 0: once keywords are in, genre adds nothing. A small weight is kept so titles with few keywords stay sensible and explanations can mention genres. Override per instance with `ContentKNN(content, weights={...})`.

**Known limitation - hub titles**: a title with very few keywords can match many profiles through one generic keyword. "Tell Me Lies" has only "based on novel or book" and is recommended to 3-4 of 6 users. Two fixes were tested and didn't improve the metrics, so neither is applied:
- shrinking blocks with few tokens
- dropping keywords that appear in more than 5-20% of titles

### `PreProcess.get_title_content(imdb_titles=None)`

One row per IMDB title with fetched TMDB details, built from `tmdb_title_details` (see [SQLite](../databases/sqlite.md)).

- **IMDB ID:** Emby's IMDB ID for the title is preferred over TMDB's, since watch history is linked through Emby's.
- **With `imdb_titles`** (the output of `imdb_get_encoded_genres()`): IMDB names, types, years, ratings, vote counts and genres are used where IMDB has the title.
- **Without it, or where IMDB lacks the title:** TMDB's name and genres are used, and `num_votes` is missing (it sorts as 0 in the tie-break).

## **Class:** UserProfiles

Per-user recommendations from watch history ("based on everything you've watched"). Each user's taste profile is the interest-weighted sum of the feature vectors of what they watched; recommendations are the unwatched titles closest to it (cosine), ordered and tie-broken like `recommend()`. Works with either model; `src/ml/main.py` uses `ContentKNN` and falls back to `GenreKNN` if no TMDB details have been fetched.

```python
from ml import PreProcess, ContentKNN, UserProfiles

pp = PreProcess()
user_titles, _ = pp.get_user_title_stats()
profiles = UserProfiles(ContentKNN(pp.get_title_content(pp.imdb_get_encoded_genres())), user_titles)
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
- Titles the model doesn't cover are skipped; `users()` shows how many titles shape each profile. `ContentKNN` covers 989 of 999 watched hours (Oct 2026); `GenreKNN` skips IMDB types other than movie/tvSeries/tvMiniSeries (e.g. `tvMovie`).

**`because_you_watched`** on each recommendation is the most similar profile title, ties going to the higher-interest title (None if nothing is shared), and **`shared`** is what the two have in common. It deliberately ignores interest otherwise, or a long binge would "explain" anything that shares one genre with it. Example: The Dark Knight Rises, because you watched The Dark Knight: `[Jonathan Nolan, Christian Bale, Christopher Nolan, ...]`.

With `GenreKNN`, many unwatched titles share the most common profile genres (e.g. `Action, Crime, Drama`), so the popularity tie-break picks the list and users with similar profiles get the same recommendations - one reason `ContentKNN` is the default.

## Evaluation: `ml.evaluate`

```sh
PYTHONPATH=src python -m ml.evaluate            # compare popularity, GenreKNN, ContentKNN and each content block alone
PYTHONPATH=src python -m ml.evaluate --grid     # also search ContentKNN block weights (--step 0.2, ~1 min)
```

The evaluation is leave-one-out on real watch history:
1. For each user, take each title they clearly liked: at least 80% of a movie, or about 1.5+ hours of a series.
2. Hide it, and build the profile from everything else they watched.
3. Rank every candidate title they haven't watched, and record where the hidden title lands.

Every model is scored on the same candidates (titles both models know) and the same profiles.

Metrics, where higher is better:
- **hit@10:** the share of hidden titles that land in the top 10
- **MRR:** mean reciprocal rank
- **AUC:** the share of other candidates ranked below the hidden title; 0.5 is random order

Results, Oct 2026 (566 candidates, 178 held out):

| Model | hit@10 | MRR | AUC |
| --- | --- | --- | --- |
| Popularity (most IMDB votes) | 0.022 | 0.018 | 0.491 |
| `GenreKNN` | 0.034 | 0.023 | 0.545 |
| **`ContentKNN`** | **0.096** | **0.042** | **0.650** |
| keywords only | 0.073 | 0.031 | 0.615 |
| people only | 0.067 | 0.049 | 0.566 |
| description only | 0.022 | 0.024 | 0.549 |

What the results show:
- Content features roughly triple hit@10 over genre-only.
- Keywords and people each carry most of the signal, and they work best combined.
- Popularity is no better than random here: what people watch on this server isn't just what's popular.

Caveats:
- The data is small: 6 people. Differences of a couple of points are noise, and some users have only 1-7 held-out titles.
- People can only watch what's on the server, so "a hidden title ranked high" is the best available proxy for a good recommendation, not the same thing.

Re-run as history grows, and use it to measure any new feature before adopting it.

Demo: `PYTHONPATH=src python src/ml/main.py [IMDB ID ...]` - title-based recommendations for the given IDs (default: Baby Driver), then per-user recommendations from the Emby library.
