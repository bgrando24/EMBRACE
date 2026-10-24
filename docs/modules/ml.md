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

Recommender over richer content than genre: TMDB keywords, people and descriptions, plus collaborative signals from MovieLens (and optionally TMDB). It only covers titles with fetched TMDB details: the library's movies and series, including removed ones still in watch history. Scoring is the same cosine similarity, ordering and tie-break as `GenreKNN`.

```python
from ml import PreProcess, ContentKNN

pp = PreProcess()
knn = ContentKNN(pp.get_title_content(pp.imdb_get_encoded_genres(), pp.get_movielens_likers()))
knn.recommend("tt3890160", n=10, allowed=pp.get_library_imdb_ids())   # Baby Driver -> The Town, Wrath of Man, Den of Thieves
```

`recommend()` returns `shared`: what each recommendation has in common with the seed. Shared keywords and people come first, ordered by how much they add to the similarity, then shared genres. Example: `[Jon Hamm, bank robbery, heist, shootout, Crime]`.

| Block | Features | Default weight |
| --- | --- | --- |
| `genre` | IMDB genres (TMDB's if IMDB has none) | 0.035 |
| `keywords` | TMDB keywords, `collection: <franchise>` for movies, `language: <code>` when not English | 0.28 |
| `people` | Director + writers (movies) or creators (series), and the top 8 billed cast, by TMDB person ID | 0.245 |
| `description` | Words of the TMDB overview, English stop words removed | 0.14 |
| `movielens` | The MovieLens users who rated the movie 4+: two movies are similar when the same people liked both | 0.3 |
| `tmdb_recs` | TMDB's "people who liked this also liked" list, plus the title itself | 0 (off) |

The two collaborative blocks bring in behaviour from far more people than this server has:
- **`movielens`** is collaborative filtering from 200k MovieLens users. It only covers movies up to Oct 2023: 245 of 454 library movies. For series and newer movies the block is empty, and the other blocks take up its weight.
- **`tmdb_recs`** is fetched with the TMDB details. It made no measurable difference at 0.1-0.3 in either evaluation, so it's off, but the data is there to re-test.

How the features are scored and combined:
- **TF-IDF weighting:** each block is TF-IDF weighted and L2-normalised on its own. A shared keyword or actor counts for more the rarer it is, so "workplace comedy" outweighs "based on novel or book".
- **Single-title tokens dropped:** a token found in only one title can never match anything, so it is left out.
- **Combining blocks:** the blocks are concatenated with `sqrt(weight)` scaling. Similarity is therefore the weighted sum of the per-block cosines, re-normalised over the blocks a title has.

**Weights** are `ContentKNN.DEFAULT_WEIGHTS`, chosen with `ml.evaluate` (below):
- Every one of the 10 best grid weightings of the content blocks gave genre 0: once keywords are in, genre adds nothing. A small weight is kept so titles with few keywords stay sensible and explanations can mention genres.
- MovieLens gets 0.3, with the content blocks scaled to the remaining 0.7. MRR rose steadily as its weight went from 0.1 to 0.5, in both evaluations.

Override per instance with `ContentKNN(content, weights={...})`.

**Known limitation - hub titles**: a title with very few keywords can match many profiles through one generic keyword. "Tell Me Lies" has only "based on novel or book" and is recommended to 3-4 of 6 users. Two fixes were tested and didn't improve the metrics, so neither is applied:
- shrinking blocks with few tokens
- dropping keywords that appear in more than 5-20% of titles

### `PreProcess.get_title_content(imdb_titles=None, movielens=None)`

One row per IMDB title with fetched TMDB details, built from `tmdb_title_details` (see [SQLite](../databases/sqlite.md)).

- **IMDB ID:** Emby's IMDB ID for the title is preferred over TMDB's, since watch history is linked through Emby's.
- **With `imdb_titles`** (the output of `imdb_get_encoded_genres()`): IMDB names, types, years, ratings, vote counts and genres are used where IMDB has the title.
- **Without it, or where IMDB lacks the title:** TMDB's name and genres are used, and `num_votes` is missing (it sorts as 0 in the tie-break).

## **Class:** UserProfiles

Per-user recommendations from watch history ("based on everything you've watched"). Each user's taste profile is the interest-weighted sum of the feature vectors of what they watched; recommendations are the unwatched titles closest to it (cosine), ordered and tie-broken like `recommend()`. Works with either model; `src/ml/main.py` uses `ContentKNN` and falls back to `GenreKNN` if no TMDB details have been fetched.

Profiles also use the titles Emby has marked played that aren't in the watch history (`PreProcess.get_played_titles()`): watched before Playback Reporting's ~90-day window, elsewhere, or marked by hand. A played movie counts as watched once, and a series by its played episodes' runtime. Like everything watched, these are never recommended. In leave-one-out they raised hit@10 from 0.096 to 0.112 and AUC from 0.650 to 0.676.

```python
from ml import PreProcess, ContentKNN, UserProfiles

pp = PreProcess()
user_titles, _ = pp.get_user_title_stats()
user_titles = pd.concat([user_titles, pp.get_played_titles(user_titles)], ignore_index=True)
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

## **Class:** Freshness

How recently each title arrived on the server, as of any date. A movie counts from when it was added; a series from when its newest episode (or the series itself) was added, so a show getting new episodes is fresh again. It's built from Emby's DateCreated (`library_items.date_created`) via `PreProcess.get_title_additions()`.

```python
from ml import Freshness

freshness = Freshness(pp.get_title_additions())
freshness.at()                     # titles on the server now: first_added, last_added, age_days
freshness.at("2026-03-01")         # ... as they were on a past date
profiles.recommend(user_id, n=10, allowed=library, boost=freshness.boost())   # new arrivals first, by taste
```

`boost(when, weight=0.3, half_life_days=30)` returns `weight * 0.5 ** (age_days / half_life_days)` per title, which `recommend_for_profile()` / `UserProfiles.recommend()` add to the similarity before ranking. They then return `boost` and `score` too.
- With the defaults, a title added today gets +0.3, more than almost any profile similarity (typically 0.1-0.3).
- So new arrivals lead, ordered by taste. After 3 months a title's boost is down to +0.04.

The demo prints two lists per user:
- **"fresh for you"** (with the boost) predicts what people will actually watch next.
- **"from the back catalogue"** (taste alone) surfaces older titles that Emby's own "Latest" row won't.

Data caveats:
- Dates are only stored from Oct 2026, so items removed before then have none and never count as available.
- The server rebuild re-imported ~840 episodes on 2026-07-24/25, so some shows look like they got new episodes then. Movies keep their file dates.

## Evaluation: `ml.evaluate`

```sh
PYTHONPATH=src python -m ml.evaluate            # compare popularity, GenreKNN, ContentKNN and each content block alone
PYTHONPATH=src python -m ml.evaluate --grid     # also search ContentKNN block weights (--step 0.2, ~1 min)
PYTHONPATH=src python -m ml.evaluate --temporal # also the time-aware next-watch evaluation + freshness boosts (~1.5 min)
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

Results, Oct 2026 (566 candidates, 178 held out, profiles including Emby played flags):

| Model | hit@10 | MRR | AUC |
| --- | --- | --- | --- |
| Popularity (most IMDB votes) | 0.022 | 0.019 | 0.488 |
| `GenreKNN` | 0.051 | 0.026 | 0.565 |
| **`ContentKNN`** (defaults) | **0.135** | **0.070** | **0.681** |
| keywords only | 0.084 | 0.054 | 0.636 |
| people only | 0.096 | 0.068 | 0.585 |
| description only | 0.028 | 0.014 | 0.562 |
| MovieLens only | 0.067 | 0.055 | 0.429 |
| TMDB recommendations only | 0.022 | 0.016 | 0.639 |

How the default model got there:

| Step | hit@10 | MRR | AUC |
| --- | --- | --- | --- |
| Keywords + people + description + genre | 0.096 | 0.042 | 0.650 |
| + Emby played flags in profiles | 0.112 | 0.059 | 0.676 |
| + MovieLens (0.3) | 0.135 | 0.070 | 0.681 |

The single-block models can't score every title (e.g. MovieLens has no series); titles a model can't score are ranked last, which is why "MovieLens only" has a low AUC.

What the results show:
- Content features more than double hit@10 over genre-only, and MovieLens adds a collaborative signal on top.
- Keywords and people each carry most of the signal, and they work best combined.
- Popularity is no better than random here: what people watch on this server isn't just what's popular.

Caveats:
- The data is small: 6 people. Differences of a couple of points are noise, and some users have only 1-7 held-out titles.
- People can only watch what's on the server, so "a hidden title ranked high" is the best available proxy for a good recommendation, not the same thing.

### Time-aware: `next_watch()`

Leave-one-out ignores time: the profile includes titles watched *after* the hidden one, and every title counts as available. That's fine for comparing content features. It can't test anything time-dependent, so `--temporal` replays history as it happened:
1. For each liked title, take the moment the user first watched it.
2. Build the profile from only what they had first watched before then.
3. Rank only titles already on the server at that point, by Emby's dates.

Results, Oct 2026 (133 held out; fewer than above, since first watches and titles without dates can't be held out):

| Model | hit@10 | MRR | AUC |
| --- | --- | --- | --- |
| Popularity | 0.030 | 0.021 | 0.453 |
| `GenreKNN` | 0.045 | 0.018 | 0.505 |
| `ContentKNN` | 0.083 | 0.033 | 0.595 |
| Newest first (ignores taste) | 0.376 | 0.172 | 0.760 |
| `ContentKNN` + freshness boost (0.3, 30 days) | 0.376 | 0.202 | 0.822 |
| **+ the user's own Jellyseerr requests (+0.1)** | **0.376** | **0.238** | **0.832** |

(Emby played flags have no dates, so they can't take part in this replay.)

What the results show:
- **Recency is the strongest single signal** on this server, for movies and series alike: half of the movies people liked were first watched within 29 days of arriving, and a quarter within 5 days.
- **Taste on top of recency still helps**: AUC goes from 0.76 to 0.82, so taste orders the rest of the catalogue much better than age does. Top-10 hits stay about the same.
- **Content alone predicts the next watch poorly.** That's expected when most watching is of new arrivals.
- **Robust settings:** results plateau for boost weights 0.2-1.0 and half-lives of 14-60 days. The defaults are mid-plateau.

Caveat: profile weights use each earlier title's total interest, including watching after the moment being replayed. It's a small leak, because only per-title totals are kept.

Re-run as history grows, and use it to measure any new feature before adopting it.

Demo: `PYTHONPATH=src python src/ml/main.py [IMDB ID ...]` - title-based recommendations for the given IDs (default: Baby Driver), then per-user recommendations from the Emby library.

## External data sources

| Source | What it adds | Setup | Licence |
| --- | --- | --- | --- |
| TMDB API | Keywords, cast/crew, descriptions, franchises, TMDB recommendations | `TMDB_READ_ACCESS_TOKEN`; fetched by the refresh | [TMDB API terms](https://www.themoviedb.org/api-terms-of-use): free, attribution required |
| Emby | Played flags and favourites (`user_item_states`), date added | none; fetched by the refresh | your server |
| MovieLens 32M | Collaborative "liked by the same people" signal for movies | `scripts/movielens/download_movielens.sh` (240 MB download, ~1 GB unpacked, in `data/movielens/`, gitignored); cached on first use in `data/cache/movielens_likers.parquet` | GroupLens: research / non-commercial use only, no endorsement implied; cite F. Maxwell Harper and Joseph A. Konstan, 2015, *The MovieLens Datasets: History and Context*, ACM TiiS 5(4) |
| Jellyseerr | Who requested what, and when (`jellyseerr_requests`, `PreProcess.get_user_requests()`, `Requests`) | `JELLYSEERR_URL` + `JELLYSEERR_API_KEY` in `.env`; fetched by the refresh once set | your server |
| IMDB datasets | Genres, ratings, vote counts, episode -> series | see [MySQL](../databases/mysql.md) | IMDB non-commercial licence |

## **Class:** Requests

Jellyseerr requests as a ranking signal, built from `PreProcess.get_user_requests()`. `boost(when, user_id, own=0.1, others=0)` gives +`own` to titles that user requested before `when`. `Recommender.picks()` adds it to the freshness boost for the fresh list, and marks those rows `you_requested`.

Evaluated on 195 requests (Dec 2025 - Oct 2026, 5 requesters):
- **What it measures:** 25 of the 133 time-aware test titles had been requested by that person beforehand. They already ranked well thanks to freshness (median rank 4), and the boost puts them first (median rank 1).
- **Result:** MRR 0.202 -> 0.238 and AUC 0.822 -> 0.832, with no change to the rest. Results plateau from 0.05 to 0.15.
- **Other people's requests:** boosting these made results worse, so `others` is 0.
- **Not used in profiles:** only about 1 in 4 requests is watched by its requester. Adding requested titles to profiles (`PreProcess.get_requested_titles()`) lowered both evaluations at every weight tried (0.25-1.0).
- **Never excluded:** requested titles are never removed from recommendations, since "the thing you asked for has arrived" is a fair recommendation.

## Daily picks: `Recommender` (`ml.picks`)

`Recommender.load(pp)` builds the default model once: ContentKNN with MovieLens when it's downloaded, falling back to GenreKNN without TMDB details. It also builds the user profiles (including Emby played flags), the library, `Freshness` and `Requests`. `src/ml/main.py` and the nightly picks job both use it.

`picks(user_id, n=10, when=None)` returns two lists, and a title is never in both:
- **`fresh`:** taste + the freshness boost + the user's own requests. What people watch next is mostly what just arrived (see [time-aware evaluation](#time-aware-next_watch)).
- **`back_catalogue`:** taste alone, leaving out the fresh list. This surfaces older titles that Emby's own "Latest" row won't.

Columns: `list`, `rank`, `t_const`, `primary_name`, `start_year`, `type`, `similarity`, `score`, `because_you_watched`, `shared`, `added_days_ago`, `you_requested`.

`all_picks()` runs `picks()` for every user with at least `MIN_PROFILE_TITLES` (5) titles in their profile, because a handful of titles is noise rather than taste.

### Nightly playlists: `scripts/picks/daily_picks.py`

Runs after the refresh (see [Scheduled Jobs](../workflows/cron_jobs.md)):
- **Emby playlists:** each eligible user gets two private playlists, "Fresh for you" and "From the back catalogue", updated in place. Emby only lists a playlist to its owner.
- **Shows:** adding a series to an Emby playlist adds every episode, so a show is represented by its first episode (`PreProcess.get_playlist_items()`).
- **Discord preview:** a summary of everyone's top 3 per list goes to `DISCORD_WEBHOOK_URL`.
- **Log:** every pick is appended to [`recommendation_log`](../databases/sqlite.md#recommendation-log), so later watch history can show which picks were actually watched.
- **Opt-out:** users named in `EMBRACE_PICKS_EXCLUDE_USERS` get no playlists, and their picks are left out of the preview.

```sh
PYTHONPATH=src python scripts/picks/daily_picks.py --dry-run     # print everyone's picks, change nothing
PYTHONPATH=src python scripts/picks/daily_picks.py --users NAME  # one user only
```
