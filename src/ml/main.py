import sys
import pandas as pd
from ml import PreProcess, GenreKNN

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


# ------------- user watch history-based recommendations

# collect the set of titles they’ve watched from Emby: pp.get_user_title_stats() (one row per user + IMDB title)
# grab their vectors from X, build a weighted average user vector:
# watched_idx = [ ... ]  # indices of titles watched
# weights = [ ... ]      # e.g. completion ratio, minutes watched

# user_vec = np.average(X[watched_idx], axis=0, weights=weights)
# user_vec = user_vec / np.linalg.norm(user_vec)  # normalize

# # query with this user vector: similarity = knn._X @ user_vec, then rank as in GenreKNN.recommend()
