import sys
import pandas as pd
from ml import PreProcess, GenreKNN, ContentKNN, UserProfiles

# usage (from project root): PYTHONPATH=src python src/ml/main.py [IMDB ID ...]
# e.g. PYTHONPATH=src python src/ml/main.py tt3890160     (Baby Driver)

pd.set_option("display.width", 200)
pd.set_option("display.max_colwidth", 60)

#---- genre model: one row per IMDB movie/series, multi-hot encoded genres (cached after the first MySQL fetch) ----
pp = PreProcess()
titles = pp.imdb_get_encoded_genres()
genre_knn = GenreKNN(titles)
library = pp.get_library_imdb_ids()
print(f"Genre model: {len(genre_knn.titles)} titles. Emby library: {len(library)} movies/series with an IMDB ID")

#---- content model: TMDB keywords, people and descriptions for library titles (fetched by the daily refresh) ----
try:
    knn = ContentKNN(pp.get_title_content(titles))
    print(f"Content model: {len(knn.titles)} titles, weights {knn.weights}")
except ValueError as e:
    print(f"Content model unavailable ({e}), using the genre model")
    knn = genre_knn

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
user_titles, _ = pp.get_user_title_stats()
profiles = UserProfiles(knn, user_titles)
print("\n=== Users")
print(profiles.users().drop(columns="user_id").round(1).to_string(index=False))
for user_id, user_name in profiles.users()[["user_id", "user_name"]].itertuples(index=False):
    shares = profiles.genre_shares(user_id).head(5)
    print(f"\n=== {user_name} - " + ", ".join(f"{g} {s:.0%}" for g, s in shares.items()))
    recs = profiles.recommend(user_id, n=10, allowed=library)
    print(recs.drop(columns=["type", "genres", "avg_rating"]).to_string(index=False))
