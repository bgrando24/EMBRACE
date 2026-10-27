"""
Offline evaluation of the recommenders on real watch history. Two set-ups:

leave_one_out()  For each user and each title they clearly liked (interest >= MIN_HELDOUT_INTEREST), hide it, build
                 their profile from everything else they watched, rank every candidate title they haven't watched, and
                 record where the hidden title lands. Ignores time: good for comparing content features.
next_watch()     The same, but as it would have happened: the profile only uses titles watched BEFORE the held-out
                 title was first watched, and only titles already on the server then are ranked. Needed for anything
                 time-dependent, such as boosting recently added titles.

A good recommender ranks titles people went on to watch near the top.

Metrics (higher is better):
    hit@10   share of held-out titles ranked in the top 10
    mrr      mean reciprocal rank (1 for rank 1, 0.5 for rank 2, ...)
    auc      share of other candidates ranked below the held-out title (0.5 = random order)

Each metric comes with a 95% bootstrap interval (`bootstrap_ci()`), and each model is compared with the default on the
same held-out titles (`paired_difference()`): a difference whose interval includes 0 is noise.

Caveats: a few hundred held-out titles from a handful of people, so differences of a couple of points are noise; and
people can only watch what's on the server, so "hidden title ranked high" is the best available proxy for "good
recommendation", not the same thing.

Usage (from project root): PYTHONPATH=src python -m ml.evaluate [--grid] [--temporal]
"""
import argparse
from pathlib import Path
import itertools
from typing import Callable, Iterable, Optional

import numpy as np
import pandas as pd

from .content_knn import ContentKNN
from .cosine_knn import CosineKNN
from .freshness import Freshness
from .requests_signal import Requests
from .genre_knn import GenreKNN
from .style_tags import StyleTags
from .preprocess import PreProcess
from .user_profiles import ABANDONED_MIN_INTEREST, MIN_INTEREST, abandoned, title_interest

# only hold out titles the user clearly liked: 80%+ of a movie (credits are often skipped), or ~1.5+ hours of a series
MIN_HELDOUT_INTEREST = 0.8

# (profile t_consts, profile weights, t_consts to exclude, candidate t_consts, as-of time, user ID) -> candidates, best first
Ranker = Callable[[list[str], list[float], set[str], list[str], Optional[pd.Timestamp], str], list[str]]
# (as-of time, user ID) -> t_const -> amount added to the similarity
Boost = Callable[[Optional[pd.Timestamp], str], pd.Series]


def profile_ranker(model: CosineKNN, boost: Optional[Boost] = None) -> Ranker:
    """
    Ranks candidates by similarity to the interest-weighted profile, like `UserProfiles.recommend()`.
    `boost(when, user_id)` (optional) returns t_const -> amount added to the similarity, e.g. freshness and requests.
    """
    def rank(t_consts, weights, exclude, candidates, when=None, user_id=None):
        try:
            profile = model.profile_vector(t_consts, weights)
        except ValueError:      # none of the profile titles are in the model
            ranked = []
        else:
            ranked = model.recommend_for_profile(
                profile, n=len(candidates), allowed=candidates, exclude=exclude, boost=boost(when, user_id) if boost else None
            )["t_const"].tolist()
        # candidates the model can't score (e.g. series in a MovieLens-only model) go last
        seen = set(ranked)
        return ranked + [t for t in candidates if t not in seen and t not in exclude]
    return rank


def popularity_ranker(num_votes: pd.Series) -> Ranker:
    """Ignores the profile: most IMDB votes first (the same tie-break the models use)"""
    votes = num_votes.fillna(0).astype(float)

    def rank(t_consts, weights, exclude, candidates, when=None, user_id=None):
        c = votes.reindex([t for t in candidates if t not in exclude]).fillna(0).rename("votes").rename_axis("t_const").reset_index()
        return c.sort_values(["votes", "t_const"], ascending=[False, True])["t_const"].tolist()
    return rank


def freshness_ranker(freshness: Freshness) -> Ranker:
    """Ignores the profile: most recently added (or given new episodes) first, as of `when`"""
    def rank(t_consts, weights, exclude, candidates, when=None, user_id=None):
        age = freshness.at(when)["age_days"]
        c = pd.DataFrame({"t_const": [t for t in candidates if t not in exclude]})
        c["age"] = c["t_const"].map(age).fillna(np.inf)
        return c.sort_values(["age", "t_const"])["t_const"].tolist()
    return rank


def leave_one_out(
    ranker: Ranker,
    user_titles: pd.DataFrame,
    in_scope: set[str],
    candidates: set[str],
    min_heldout_interest: float = MIN_HELDOUT_INTEREST,
    negative_weight: Optional[float] = None,
    abandoned_min_interest: float = ABANDONED_MIN_INTEREST,
) -> pd.DataFrame:
    """
    Args:
        ranker: see `profile_ranker()` / `popularity_ranker()`
        user_titles (DataFrame): output of `PreProcess.get_user_title_stats()[0]`
        in_scope (set[str]): titles that may be part of a profile - use the same set for every model compared
        candidates (set[str]): titles that may be ranked (and so held out)
        negative_weight (float, optional): as for `UserProfiles`: abandoned titles (`abandoned()`) leave the profile
            and count against it with this weight - DEFAULT: None, treated like any other title
        abandoned_min_interest (float, optional): passed to `abandoned()`

    Returns:
        DataFrame: one row per held-out (user, title): user_name, imdb_id, unit_name, interest, rank (1 = top), n_ranked
    """
    h = user_titles.assign(
        interest=title_interest(user_titles),
        abandoned=abandoned(user_titles, min_interest=abandoned_min_interest) if negative_weight is not None else False,
    )
    rows = []
    for (user_id, user_name), u in h.groupby(["user_id", "user_name"], dropna=False):
        profile = u[u["imdb_id"].isin(in_scope) & (u["interest"] >= MIN_INTEREST) & ~u["abandoned"]]
        negatives = u.loc[u["imdb_id"].isin(in_scope) & u["abandoned"], "imdb_id"].tolist()
        watched = set(u.loc[_seen(u), "imdb_id"])
        # only watch-history titles are held out, so extra profile sources (e.g. Emby played flags) can be compared
        # on the same held-out set
        holdable = profile["source"].isna() if "source" in profile else pd.Series(True, index=profile.index)
        for _, held in profile[holdable & (profile["interest"] >= min_heldout_interest) & profile["imdb_id"].isin(candidates)].iterrows():
            rest = profile[profile["imdb_id"] != held["imdb_id"]]
            if rest.empty:
                continue
            exclude = watched - {held["imdb_id"]}
            t_consts, weights = _with_negatives(rest, negatives, negative_weight)
            ranking = ranker(t_consts, weights, exclude, sorted(candidates), None, user_id)
            rows.append({
                "user_name": user_name, "imdb_id": held["imdb_id"], "unit_name": held["unit_name"],
                "interest": held["interest"], "rank": ranking.index(held["imdb_id"]) + 1, "n_ranked": len(ranking),
            })
    return pd.DataFrame(rows)


def next_watch(
    ranker: Ranker,
    user_titles: pd.DataFrame,
    in_scope: set[str],
    freshness: Freshness,
    min_heldout_interest: float = MIN_HELDOUT_INTEREST,
    negative_weight: Optional[float] = None,
    abandoned_min_interest: float = ABANDONED_MIN_INTEREST,
) -> pd.DataFrame:
    """
    Time-aware leave-one-out: for each title a user clearly liked, at the time T they first watched it, the profile
    is what they had first watched before T, and the candidates are the in-scope titles on the server at T (per
    `freshness`) that they hadn't watched yet.

    Held-out titles that weren't on the server at T by Emby's dates (e.g. re-imported on the rebuild) are skipped,
    as are users' first titles (no earlier history). Profile weights use the user's total interest in each earlier
    title, including any watching after T - a small leak, as only totals are kept per title. With `negative_weight`
    (see `leave_one_out()`), titles count as abandoned by how things stood at T.

    Returns:
        DataFrame: as `leave_one_out()`, plus `watched_at` (T)
    """
    h = user_titles.assign(
        interest=title_interest(user_titles),
        first_watched=pd.to_datetime(user_titles["first_watched_timestamp"], errors="coerce"),
    )
    # rows from other sources join the profile from when they happened (e.g. a Jellyseerr request), else never
    known_at = h["first_watched"]
    if "requested_at" in h:
        known_at = known_at.fillna(pd.to_datetime(h["requested_at"], errors="coerce"))
    h["known_at"] = known_at
    rows = []
    for (user_id, user_name), u in h.groupby(["user_id", "user_name"], dropna=False):
        pool = u[u["imdb_id"].isin(in_scope) & (u["interest"] >= MIN_INTEREST)]
        holdable = pool["source"].isna() if "source" in pool else pd.Series(True, index=pool.index)
        for _, held in pool[holdable & (pool["interest"] >= min_heldout_interest)].iterrows():
            when = held["first_watched"]
            if pd.isna(when):
                continue
            earlier = pool[(pool["known_at"] < when) & (pool["imdb_id"] != held["imdb_id"])]
            negatives = []
            if negative_weight is not None:
                gave_up = abandoned(u, now=when, min_interest=abandoned_min_interest) & u["imdb_id"].isin(in_scope)
                earlier = earlier[~earlier.index.isin(u.index[gave_up])]
                negatives = u.loc[gave_up, "imdb_id"].tolist()
            available = set(freshness.at(when).index) & in_scope
            if earlier.empty or held["imdb_id"] not in available:
                continue
            exclude = set(u.loc[_seen(u) & (u["first_watched"] < when), "imdb_id"])
            candidates = sorted(available - exclude)
            t_consts, weights = _with_negatives(earlier, negatives, negative_weight)
            ranking = ranker(t_consts, weights, exclude, candidates, when, user_id)
            rows.append({
                "user_name": user_name, "imdb_id": held["imdb_id"], "unit_name": held["unit_name"],
                "interest": held["interest"], "watched_at": when,
                "rank": ranking.index(held["imdb_id"]) + 1, "n_ranked": len(ranking),
            })
    return pd.DataFrame(rows)


def _with_negatives(profile: pd.DataFrame, negatives: list[str],
                    negative_weight: Optional[float]) -> tuple[list[str], list[float]]:
    """A ranker's profile titles and weights: by interest, then the abandoned titles at -negative_weight"""
    t_consts, weights = profile["imdb_id"].tolist(), profile["interest"].tolist()
    if negative_weight:
        t_consts, weights = t_consts + negatives, weights + [-negative_weight] * len(negatives)
    return t_consts, weights


def _seen(user_rows: pd.DataFrame) -> pd.Series:
    """Rows meaning the user has seen the title (so it's never recommended): all but Jellyseerr requests"""
    if "source" not in user_rows:
        return pd.Series(True, index=user_rows.index)
    return user_rows["source"].ne("jellyseerr_request")


def summarise(results: pd.DataFrame, k: int = 10) -> dict[str, float]:
    """hit@k, MRR and AUC over all held-out titles"""
    r = results["rank"].to_numpy(dtype=float)
    n = results["n_ranked"].to_numpy(dtype=float)
    return {"held_out": len(results), **{m: _metric(r, n, m, k) for m in (f"hit@{k}", "mrr", "auc")}}


def _metric(rank: np.ndarray, n_ranked: np.ndarray, metric: str, k: int = 10) -> float:
    if metric == f"hit@{k}":
        return float(np.mean(rank <= k))
    if metric == "mrr":
        return float(np.mean(1 / rank))
    if metric == "auc":
        return float(np.mean((n_ranked - rank) / np.maximum(n_ranked - 1, 1)))
    raise ValueError(f"unknown metric {metric}")


def bootstrap_ci(results: pd.DataFrame, metric: str = "mrr", n: int = 1000, seed: int = 0) -> tuple[float, float]:
    """
    95% interval of a metric (hit@10, mrr or auc) from resampling the held-out titles. Titles are resampled, not
    users: with 6 users, resampling users would mostly measure who's in the sample.
    """
    r, nr = results["rank"].to_numpy(dtype=float), results["n_ranked"].to_numpy(dtype=float)
    idx = np.random.default_rng(seed).integers(0, len(r), size=(n, len(r)))
    stats = [_metric(r[i], nr[i], metric) for i in idx]
    return float(np.percentile(stats, 2.5)), float(np.percentile(stats, 97.5))


def paired_difference(a: pd.DataFrame, b: pd.DataFrame, metric: str = "mrr", n: int = 1000,
                      seed: int = 0) -> tuple[float, float, float]:
    """
    How much better model `a` is than model `b` on the same held-out titles (results of the same evaluation): the
    difference in `metric` and its 95% bootstrap interval. Pairing removes the noise of which titles were held out,
    so this is far more sensitive than comparing two intervals.
    """
    keys = ["user_name", "imdb_id"] + (["watched_at"] if "watched_at" in a and "watched_at" in b else [])
    m = a.merge(b, on=keys, suffixes=("_a", "_b"))
    ra, rb = m["rank_a"].to_numpy(dtype=float), m["rank_b"].to_numpy(dtype=float)
    na, nb = m["n_ranked_a"].to_numpy(dtype=float), m["n_ranked_b"].to_numpy(dtype=float)
    diff = _metric(ra, na, metric) - _metric(rb, nb, metric)
    idx = np.random.default_rng(seed).integers(0, len(m), size=(n, len(m)))
    stats = [_metric(ra[i], na[i], metric) - _metric(rb[i], nb[i], metric) for i in idx]
    return diff, float(np.percentile(stats, 2.5)), float(np.percentile(stats, 97.5))


def with_intervals(results: dict[str, pd.DataFrame], baseline: str) -> pd.DataFrame:
    """`summarise()` per model, with 95% intervals for hit@10 and MRR, and the MRR difference from `baseline`"""
    rows = {}
    for name, res in results.items():
        row = summarise(res)
        for metric in ("hit@10", "mrr"):
            lo, hi = bootstrap_ci(res, metric)
            row[metric] = f"{row[metric]:.3f} ({lo:.3f}-{hi:.3f})"
        row["auc"] = f"{row['auc']:.3f}"
        if name != baseline and baseline in results:
            d, lo, hi = paired_difference(res, results[baseline])
            row[f"mrr vs {baseline}"] = f"{d:+.3f} ({lo:+.3f} to {hi:+.3f})" + (" *" if lo > 0 or hi < 0 else "")
        rows[name] = row
    out = pd.DataFrame(rows).T
    out["held_out"] = out["held_out"].astype(int)
    return out.fillna("")


def weight_grid(step: float = 0.1) -> list[dict[str, float]]:
    """Every ContentKNN block weighting on a grid that sums to 1"""
    blocks = list(ContentKNN.DEFAULT_WEIGHTS)
    units = round(1 / step)
    return [
        {b: round(c * step, 3) for b, c in zip(blocks, combo)}
        for combo in itertools.product(range(units + 1), repeat=len(blocks)) if sum(combo) == units
    ]


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--grid", action="store_true", help="also search ContentKNN block weights")
    parser.add_argument("--temporal", action="store_true", help="also run the time-aware next-watch evaluation, "
                        "incl. freshness boosts (needs library_items.date_created)")
    parser.add_argument("--step", type=float, default=0.2, help="grid step for --grid (DEFAULT: 0.2)")
    parser.add_argument("--db", help="SQLite database name (DEFAULT: SQLITE_DB_NAME)")
    args = parser.parse_args()

    pp = PreProcess(args.db)
    imdb = pp.imdb_get_encoded_genres()
    movielens = pp.get_movielens_likers() if Path(pp.MOVIELENS_DIR).exists() or Path("data/cache/movielens_likers.parquet").exists() else None
    content = StyleTags().attach(pp.get_title_content(imdb, movielens))    # untagged titles have no style tokens
    user_titles, _ = pp.get_user_title_stats()
    # Emby's played flags fill in profiles (never held out - see leave_one_out)
    user_titles = pd.concat([user_titles, pp.get_played_titles(user_titles)], ignore_index=True)
    genre_knn = GenreKNN(imdb)
    content_knn = ContentKNN(content)

    # compare like with like: profiles and candidates limited to titles both models know
    scope = {t for t in content["t_const"] if t in genre_knn and t in content_knn}
    votes = content.set_index("t_const")["num_votes"]
    models = {
        "popularity": popularity_ranker(votes),
        "genre": profile_ranker(genre_knn),
        "content": profile_ranker(content_knn),
    }
    for block in ContentKNN.DEFAULT_WEIGHTS:
        models[f"content: {block} only"] = profile_ranker(ContentKNN(content, {b: float(b == block) for b in ContentKNN.DEFAULT_WEIGHTS}))

    print(f"Candidates: {len(scope)} titles. Held out: titles with interest >= {MIN_HELDOUT_INTEREST}.\n")
    results = {name: leave_one_out(r, user_titles, scope, scope) for name, r in models.items()}
    print("hit@10 and MRR with 95% intervals; * = the difference from the default model isn't noise\n")
    print(with_intervals(results, "content").to_string())

    per_user = pd.DataFrame({
        name: res.groupby("user_name")["rank"].apply(lambda r: np.mean(r <= 10)) for name, res in results.items()
        if name in ("popularity", "genre", "content")
    })
    per_user["held_out"] = results["content"].groupby("user_name").size()
    print("\nhit@10 per user:")
    print(per_user.round(2).to_string())

    if args.grid:
        print("\nContentKNN weight grid (top 10 by MRR):")
        rows = []
        for w in weight_grid(args.step):
            res = leave_one_out(profile_ranker(ContentKNN(content, w)), user_titles, scope, scope)
            rows.append({**w, **summarise(res)})
        grid = pd.DataFrame(rows).sort_values("mrr", ascending=False)
        print(grid.head(10).round(3).to_string(index=False))
        default = grid[(grid[list(ContentKNN.DEFAULT_WEIGHTS)] == pd.Series(ContentKNN.DEFAULT_WEIGHTS)).all(axis=1)]
        print(f"\nDefault weights rank {grid.index.get_indexer(default.index)[0] + 1 if not default.empty else '-'} of {len(grid)}")


    if args.temporal:
        freshness = Freshness(pp.get_title_additions())
        if not len(freshness.at()):
            print("\nNo library_items.date_created yet - run the refresh to store Emby's DateCreated")
            return
        print("\nNext-watch (time-aware) evaluation:")
        temporal = {
            "popularity": popularity_ranker(votes),
            "newest first": freshness_ranker(freshness),
            "genre": profile_ranker(genre_knn),
            "content": profile_ranker(content_knn),
        }
        for weight in (0.1, Freshness.DEFAULT_WEIGHT, 1.0):
            for half_life in (14, Freshness.DEFAULT_HALF_LIFE_DAYS, 60):
                temporal[f"content + fresh {weight} / {half_life}d"] = profile_ranker(
                    content_knn, lambda when, user_id, w=weight, hl=half_life: freshness.boost(when, w, hl)
                )
        requests = Requests(pp.get_user_requests())
        temporal["content + fresh + own requests (defaults)"] = profile_ranker(
            content_knn, lambda when, user_id: freshness.boost(when).add(requests.boost(when, user_id), fill_value=0)
        )
        results = {name: next_watch(r, user_titles, scope, freshness) for name, r in temporal.items()}
        print(with_intervals(results, "content + fresh + own requests (defaults)").to_string())


if __name__ == "__main__":
    main()
