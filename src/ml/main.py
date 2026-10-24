import sys
import pandas as pd
from ml import PreProcess, Recommender
from ml.picks import BACK_CATALOGUE, FRESH

# usage (from project root): PYTHONPATH=src python src/ml/main.py [IMDB ID ...]
# e.g. PYTHONPATH=src python src/ml/main.py tt3890160     (Baby Driver)

pd.set_option("display.width", 200)
pd.set_option("display.max_colwidth", 60)

#---- the default model (ContentKNN with MovieLens, else GenreKNN) + profiles, freshness and requests ----
pp = PreProcess()
rec = Recommender.load(pp)
knn, library = rec.knn, rec.library
print(f"{type(knn).__name__}: {len(knn.titles)} titles. Emby library: {len(library)} movies/series with an IMDB ID")

# ------------- title-based recommendations -> "because you watched X, here are similar titles."
seeds = sys.argv[1:] or ["tt3890160"]
for seed in seeds:
    if seed not in knn:
        print(f"\n{seed}: not in the model")
        continue
    name = knn.titles.loc[knn.titles["t_const"] == seed, "primary_name"].iloc[0]
    print(f"\n=== {name} ({seed}) - genres: {knn.genres_of(seed)}")
    print(knn.recommend(seed, n=10, allowed=library).drop(columns="type").to_string(index=False))

# ------------- watch history-based recommendations -> "based on everything you've watched, try these."
# Two lists (see ml.picks): "fresh" ranks new arrivals by taste, because what people actually watch next is mostly
# what just arrived (see docs/modules/ml.md#evaluation); "back catalogue" is taste alone, minus the fresh list.
# The same picks go into Emby playlists nightly: scripts/picks/daily_picks.py
print("\n=== Users")
print(rec.profiles.users().drop(columns="user_id").round(1).to_string(index=False))
for user_id, user_name in rec.profiles.users()[["user_id", "user_name"]].itertuples(index=False):
    shares = rec.profiles.genre_shares(user_id).head(5)
    print(f"\n=== {user_name} - " + ", ".join(f"{g} {s:.0%}" for g, s in shares.items()))
    picks = rec.picks(user_id, n=10)
    for lst, cols in [(FRESH, ["added_days_ago", "you_requested"]), (BACK_CATALOGUE, [])]:
        mine = picks[picks["list"] == lst]
        if len(mine):
            print(f"-- {lst.replace('_', ' ')}:")
            print(mine[["primary_name", "start_year", "similarity"] + cols + ["because_you_watched", "shared"]].head(5 if lst == FRESH else 10).to_string(index=False))
