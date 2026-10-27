"""
What happened to the daily picks: was each recommended title watched (or, for titles to request, requested) while it
was recommended or soon after? The offline evaluation (`ml.evaluate`) can only replay history; this measures the picks
people were actually shown, from `recommendation_log`.

Each (user, list, title) counts once, however many days it was recommended. Its window runs from the first day it
was recommended to `window_days` after the last. A pick is `pending` until that window has closed; rates only count
closed windows (counting picks watched early but not the rest would inflate them), and open ones are shown apart.

Watching a pick says little on its own, since new arrivals get watched anyway. So each pick is compared with a random
title: the share of the titles on the server that the user hadn't watched which they started in the same window.
`lift` is watched / that expectation: 3.0 means picks were watched three times as often as a random title.

Usage (from project root): PYTHONPATH=src python -m ml.outcomes [--window 14] [--by-user]
"""
import argparse
from typing import Optional

import numpy as np
import pandas as pd

from .evaluate import MIN_HELDOUT_INTEREST
from .freshness import Freshness
from .user_profiles import title_interest

WINDOW_DAYS = 14
# a pick counts as liked at the bar the evaluation holds titles out at: 80%+ of a movie, ~1.5+ hours of a series
LIKED_INTEREST = MIN_HELDOUT_INTEREST


def pick_outcomes(log: pd.DataFrame, user_titles: pd.DataFrame, requests: pd.DataFrame,
                  freshness: Optional[Freshness] = None, window_days: int = WINDOW_DAYS, now=None) -> pd.DataFrame:
    """
    Args:
        log (DataFrame): `SQLiteConnector.get_recommendation_log()`
        user_titles (DataFrame): `PreProcess.get_user_title_stats()[0]` (watch history; first_watched_timestamp)
        requests (DataFrame): `PreProcess.get_user_requests()` (user_id, t_const, requested_at)
        freshness (Freshness, optional): what was on the server when, for the random-title baseline - DEFAULT: none
        now (optional): DEFAULT: now

    Returns:
        DataFrame: one row per (user_id, list, t_const): first_offered, last_offered, days_offered, best_rank,
        watched (started in the window), liked (watched, with interest >= LIKED_INTEREST), requested (by the user, in
        the window), pending (window still open), expected (chance a random unwatched title on the
        server was started in the same window; NaN without `freshness` or for titles not on the server)
    """
    now = pd.Timestamp.now() if now is None else pd.Timestamp(now)
    window = pd.Timedelta(days=window_days)
    cols = ["user_id", "list", "t_const", "first_offered", "last_offered", "days_offered", "best_rank", "watched",
            "liked", "requested", "pending", "expected"]
    if log.empty:
        return pd.DataFrame(columns=cols)
    log = log.assign(generated_at=pd.to_datetime(log["generated_at"]))
    picks = (log.groupby(["user_id", "list", "t_const"], as_index=False)
             .agg(first_offered=("generated_at", "min"), last_offered=("generated_at", "max"),
                  days_offered=("generated_at", lambda d: d.dt.date.nunique()), best_rank=("rank", "min")))
    ends = picks["last_offered"] + window

    h = user_titles.assign(interest=title_interest(user_titles),
                           first_watched=pd.to_datetime(user_titles["first_watched_timestamp"], errors="coerce"))
    h = h.groupby(["user_id", "imdb_id"], as_index=False).agg(first_watched=("first_watched", "min"), interest=("interest", "max"))
    w = picks.merge(h, how="left", left_on=["user_id", "t_const"], right_on=["user_id", "imdb_id"])
    picks["watched"] = (w["first_watched"] >= picks["first_offered"]) & (w["first_watched"] <= ends)
    picks["liked"] = picks["watched"] & (w["interest"].fillna(0) >= LIKED_INTEREST)

    r = requests.dropna(subset=["user_id", "t_const"]).assign(requested_at=lambda d: pd.to_datetime(d["requested_at"]))
    r = r.groupby(["user_id", "t_const"], as_index=False)["requested_at"].min()
    q = picks.merge(r, how="left", on=["user_id", "t_const"])["requested_at"]
    picks["requested"] = (q >= picks["first_offered"]) & (q <= ends)
    picks["pending"] = ends > now

    picks["expected"] = np.nan
    if freshness is not None:
        cache: dict = {}
        for i, p in picks.iterrows():
            key = (p["user_id"], p["first_offered"].normalize(), min(ends[i], now).normalize())
            if key not in cache:
                cache[key] = _base_rate(h[h["user_id"] == p["user_id"]], freshness, *key[1:])
            on_server, rate = cache[key]
            if p["t_const"] in on_server:
                picks.at[i, "expected"] = rate
    return picks[cols]


def _base_rate(history: pd.DataFrame, freshness: Freshness, start, end) -> tuple[set, float]:
    """Titles on the server at `start`, and the share of those the user hadn't watched that they started by `end`"""
    on_server = set(freshness.at(start).index)
    seen_before = set(history.loc[history["first_watched"] < start, "imdb_id"])
    unwatched = on_server - seen_before
    started = set(history.loc[(history["first_watched"] >= start) & (history["first_watched"] <= end), "imdb_id"])
    return on_server, len(started & unwatched) / len(unwatched) if unwatched else np.nan


def summarise_outcomes(picks: pd.DataFrame, by: tuple = ("list",)) -> pd.DataFrame:
    """
    Per `by` group: picks whose window has closed, with counts, the watched rate and lift; then picks still in their
    window (`open`) and how many of those were watched so far (`open_watched`)
    """
    if picks.empty:
        return pd.DataFrame()
    keys = list(by)
    done = picks[~picks["pending"]]
    g = done.groupby(keys)
    still = picks[picks["pending"]].groupby(keys)
    index = picks.groupby(keys).size().index
    out = pd.DataFrame({
        "picks": g.size(),
        "watched": g["watched"].sum(),
        "liked": g["liked"].sum(),
        "requested": g["requested"].sum(),
    }).reindex(index).fillna(0).astype(int)
    out["watched_rate"] = out["watched"] / out["picks"].where(out["picks"] > 0)
    expected = g["expected"].sum(min_count=1).reindex(index)
    out["lift"] = out["watched"] / expected.where(expected > 0)
    out["open"] = still.size().reindex(index).fillna(0).astype(int)
    out["open_watched"] = still["watched"].sum().reindex(index).fillna(0).astype(int)
    return out


def report_lines(summary: pd.DataFrame, labels: Optional[dict] = None) -> list[str]:
    """
    Short lines for a notification, one per list, e.g. 'new: 4 of 30 watched (13%, 3.1x a random title), 2 liked'.
    Lists with no closed windows yet are left out.
    """
    lines = []
    for lst, s in summary.iterrows():
        name = (labels or {}).get(lst, lst)
        if s["picks"] == 0:
            continue
        if lst == "to_request":
            lines.append(f"{name}: {int(s['requested'])} of {int(s['picks'])} requested, {int(s['watched'])} watched")
            continue
        lift = f", {s['lift']:.1f}x a random title" if pd.notna(s["lift"]) else ""
        lines.append(f"{name}: {int(s['watched'])} of {int(s['picks'])} watched ({s['watched_rate']:.0%}{lift}), {int(s['liked'])} liked")
    return lines


def main():
    from .preprocess import PreProcess

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--window", type=int, default=WINDOW_DAYS, help=f"days after a pick (DEFAULT: {WINDOW_DAYS})")
    parser.add_argument("--by-user", action="store_true", help="also break the summary down by user")
    parser.add_argument("--db", help="SQLite database name (DEFAULT: SQLITE_DB_NAME)")
    args = parser.parse_args()

    pp = PreProcess(args.db)
    log = pp.sqlite.get_recommendation_log()
    user_titles, _ = pp.get_user_title_stats()
    picks = pick_outcomes(log, user_titles, pp.get_user_requests(), Freshness(pp.get_title_additions()), args.window)
    if picks.empty:
        print("Nothing logged yet: the daily picks job logs every run")
        return
    print(f"{len(picks)} picks from {picks['first_offered'].min():%d %b} to {picks['last_offered'].max():%d %b}, "
          f"{int(picks['pending'].sum())} still in their {args.window}-day window\n")
    summary = summarise_outcomes(picks)
    print(summary.round(3).to_string())
    if args.by_user and not summary.empty:
        names = pp.sqlite.get_watch_hist_users().drop_duplicates("user_id").set_index("user_id")["user_name"]
        by_user = summarise_outcomes(picks.assign(user=picks["user_id"].map(names).fillna(picks["user_id"])), ("user", "list"))
        print("\n" + by_user.round(3).to_string())


if __name__ == "__main__":
    main()
