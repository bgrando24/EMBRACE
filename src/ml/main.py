import sys
import pandas as pd
from ml import PreProcess, GenreKNN, UserGenreProfiles

# usage (from project root): PYTHONPATH=src python src/ml/main.py [IMDB ID ...]
# e.g. PYTHONPATH=src python src/ml/main.py tt4647692     (Letterkenny)

pd.set_option("display.width", 200)
pd.set_option("display.max_colwidth", 40)

#---- one row per movie/series, multi-hot encoded genres (cached after the first MySQL fetch) ----
pp = PreProcess()
titles = pp.imdb_get_encoded_genres()
knn = GenreKNN(titles)
print(f"Model: {len(knn.titles)} titles, genres: {[g.removeprefix('genre_') for g in knn.genre_cols]}")

library = pp.get_library_imdb_ids()
print(f"Emby library: {len(library)} movies/series with an IMDB ID, {sum(i in knn for i in library)} in the model")

# ------------- title-based recommendations -> "because you watched X, here are similar titles."
seeds = sys.argv[1:] or ["tt4647692"]
for seed in seeds:
    if seed not in knn:
        print(f"\n{seed}: not in the model (unknown, not a movie/series, or has no genres)")
        continue
    name = knn.titles.loc[knn.titles["t_const"] == seed, "primary_name"].iloc[0]
    print(f"\n=== {name} ({seed}) - genres: {knn.genres_of(seed)}")
    print("-- anywhere on IMDB:")
    print(knn.recommend(seed, n=10).to_string(index=False))
    print("-- on the Emby server:")
    print(knn.recommend(seed, n=10, allowed=library).to_string(index=False))


# ------------- watch history-based recommendations -> "based on everything you've watched, try these."
user_titles, _ = pp.get_user_title_stats()
profiles = UserGenreProfiles(knn, user_titles)
print("\n=== Users")
print(profiles.users().drop(columns="user_id").round(1).to_string(index=False))
for user_id, user_name in profiles.users()[["user_id", "user_name"]].itertuples(index=False):
    shares = profiles.genre_shares(user_id).head(5)
    print(f"\n=== {user_name} - " + ", ".join(f"{g} {s:.0%}" for g, s in shares.items()))
    recs = profiles.recommend(user_id, n=10, allowed=library)
    print(recs.drop(columns=["type", "genres"]).to_string(index=False))
