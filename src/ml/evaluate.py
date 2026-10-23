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

Caveats: a few hundred held-out titles from a handful of people, so differences of a couple of points are noise; and
people can only watch what's on the server, so "hidden title ranked high" is the best available proxy for "good
recommendation", not the same thing.

Usage (from project root): PYTHONPATH=src python -m ml.evaluate [--grid] [--temporal]
"""
import argparse
import itertools
from typing import Callable, Iterable, Optional

import numpy as np
import pandas as pd

from .content_knn import ContentKNN
from .cosine_knn import CosineKNN
from .freshness import Freshness
from .genre_knn import GenreKNN
from .preprocess import PreProcess
from .user_profiles import MIN_INTEREST, title_interest

# only hold out titles the user clearly liked: 80%+ of a movie (credits are often skipped), or ~1.5+ hours of a series
MIN_HELDOUT_INTEREST = 0.8

# (profile t_consts, profile weights, t_consts to exclude, candidate t_consts, as-of time) -> candidates, best first
Ranker = Callable[[list[str], list[float], set[str], list[str], Optional[pd.Timestamp]], list[str]]


def profile_ranker(model: CosineKNN, boost: Optional[Callable[[Optional[pd.Timestamp]], pd.Series]] = None) -> Ranker:
    """
    Ranks candidates by similarity to the interest-weighted profile, like `UserProfiles.recommend()`.
    `boost(when)` (optional) returns t_const -> amount added to the similarity, e.g. `Freshness.boost`.
    """
    def rank(t_consts, weights, exclude, candidates, when=None):
        profile = model.profile_vector(t_consts, weights)
        recs = model.recommend_for_profile(
            profile, n=len(candidates), allowed=candidates, exclude=exclude, boost=boost(when) if boost else None
        )
        return recs["t_const"].tolist()
    return rank


def popularity_ranker(num_votes: pd.Series) -> Ranker:
    """Ignores the profile: most IMDB votes first (the same tie-break the models use)"""
    votes = num_votes.fillna(0).astype(float)

    def rank(t_consts, weights, exclude, candidates, when=None):
        c = votes.reindex([t for t in candidates if t not in exclude]).fillna(0).rename("votes").rename_axis("t_const").reset_index()
        return c.sort_values(["votes", "t_const"], ascending=[False, True])["t_const"].tolist()
    return rank


def freshness_ranker(freshness: Freshness) -> Ranker:
    """Ignores the profile: most recently added (or given new episodes) first, as of `when`"""
    def rank(t_consts, weights, exclude, candidates, when=None):
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
) -> pd.DataFrame:
    """
    Args:
        ranker: see `profile_ranker()` / `popularity_ranker()`
        user_titles (DataFrame): output of `PreProcess.get_user_title_stats()[0]`
        in_scope (set[str]): titles that may be part of a profile - use the same set for every model compared
        candidates (set[str]): titles that may be ranked (and so held out)

    Returns:
        DataFrame: one row per held-out (user, title): user_name, imdb_id, unit_name, interest, rank (1 = top), n_ranked
    """
    h = user_titles.assign(interest=title_interest(user_titles))
    rows = []
    for (user_id, user_name), u in h.groupby(["user_id", "user_name"], dropna=False):
        profile = u[u["imdb_id"].isin(in_scope) & (u["interest"] >= MIN_INTEREST)]
        watched = set(u["imdb_id"])
        for _, held in profile[(profile["interest"] >= min_heldout_interest) & profile["imdb_id"].isin(candidates)].iterrows():
            rest = profile[profile["imdb_id"] != held["imdb_id"]]
            if rest.empty:
                continue
            exclude = watched - {held["imdb_id"]}
            ranking = ranker(rest["imdb_id"].tolist(), rest["interest"].tolist(), exclude, sorted(candidates), None)
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
) -> pd.DataFrame:
    """
    Time-aware leave-one-out: for each title a user clearly liked, at the time T they first watched it, the profile
    is what they had first watched before T, and the candidates are the in-scope titles on the server at T (per
    `freshness`) that they hadn't watched yet.

    Held-out titles that weren't on the server at T by Emby's dates (e.g. re-imported on the rebuild) are skipped,
    as are users' first titles (no earlier history). Profile weights use the user's total interest in each earlier
    title, including any watching after T - a small leak, as only totals are kept per title.

    Returns:
        DataFrame: as `leave_one_out()`, plus `watched_at` (T)
    """
    h = user_titles.assign(
        interest=title_interest(user_titles),
        first_watched=pd.to_datetime(user_titles["first_watched_timestamp"], errors="coerce"),
    )
    rows = []
    for (user_id, user_name), u in h.groupby(["user_id", "user_name"], dropna=False):
        pool = u[u["imdb_id"].isin(in_scope) & (u["interest"] >= MIN_INTEREST)]
        for _, held in pool[pool["interest"] >= min_heldout_interest].iterrows():
            when = held["first_watched"]
            if pd.isna(when):
                continue
            earlier = pool[pool["first_watched"] < when]
            available = set(freshness.at(when).index) & in_scope
            if earlier.empty or held["imdb_id"] not in available:
                continue
            exclude = set(u.loc[u["first_watched"] < when, "imdb_id"])
            candidates = sorted(available - exclude)
            ranking = ranker(earlier["imdb_id"].tolist(), earlier["interest"].tolist(), exclude, candidates, when)
            rows.append({
                "user_name": user_name, "imdb_id": held["imdb_id"], "unit_name": held["unit_name"],
                "interest": held["interest"], "watched_at": when,
                "rank": ranking.index(held["imdb_id"]) + 1, "n_ranked": len(ranking),
            })
    return pd.DataFrame(rows)


def summarise(results: pd.DataFrame, k: int = 10) -> dict[str, float]:
    """hit@k, MRR and AUC over all held-out titles"""
    r = results["rank"].to_numpy(dtype=float)
    n = results["n_ranked"].to_numpy(dtype=float)
    return {
        "held_out": len(results),
        f"hit@{k}": float(np.mean(r <= k)),
        "mrr": float(np.mean(1 / r)),
        "auc": float(np.mean((n - r) / np.maximum(n - 1, 1))),
    }


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
    content = pp.get_title_content(imdb)
    user_titles, _ = pp.get_user_title_stats()
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
    summary = pd.DataFrame({name: summarise(res) for name, res in results.items()}).T
    print(summary.round(3).to_string())

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
        for weight in (0.02, 0.05, 0.1, 0.2):
            for half_life in (7, 30, 90):
                temporal[f"content + fresh {weight} / {half_life}d"] = profile_ranker(
                    content_knn, lambda when, w=weight, hl=half_life: freshness.boost(when, w, hl)
                )
        rows = {name: summarise(next_watch(r, user_titles, scope, freshness)) for name, r in temporal.items()}
        print(pd.DataFrame(rows).T.round(3).to_string())


if __name__ == "__main__":
    main()
