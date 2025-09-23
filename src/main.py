from dotenv import load_dotenv
import os
import pandas as pd
import numpy as np
from sklearn.preprocessing import normalize
from sklearn.neighbors import NearestNeighbors
from typing import Final

from connectors import EmbyConnector, SQLiteConnector, TMDBConnector
from ml import PreProcess

# load and extract env variables
load_dotenv()
BASE_DOMAIN: Final = os.getenv("BASE_DOMAIN")
EMBY_API_KEY: Final = os.getenv("EMBY_API_KEY")
TMDB_READ_ACCESS_TOKEN: Final = os.getenv("TMDB_READ_ACCESS_TOKEN")
ENVIRONMENT: Final = os.getenv("ENVIRONMENT") or "dev"
SQLITE_DB_NAME: Final = os.getenv("SQLITE_DB_NAME") or "EMBRACE_SQLITE_DB.db"

# init API connectors
# Emby = EmbyConnector(debug=(ENVIRONMENT == "dev"))
# TMDB = TMDBConnector(TMDB_READ_ACCESS_TOKEN, debug=(ENVIRONMENT == "dev"))

# # testing functions
# users = Emby.get_all_emby_users()
# print(users)
# bgmd_hist = Emby.get_user_watch_hist(users["bgmd"], 10)
# print(bgmd_hist)
# all_user_hist = Emby.get_all_watch_hist(1)
# print(all_user_hist)

# # test db connection and watch history tables
# sqlite = SQLiteConnector(SQLITE_DB_NAME, debug=True)
# try: 
#     os.remove("sqlite_db/bgmd_db.db")
# except:
#     pass

# sqlite.connect_db()

# # ingest library metadata first so runtime is available for later calculations
# sqlite._INIT_create_library_items_schema()
# ok = sqlite.ingest_all_library_items(Emby.iter_all_items(), Emby.get_item_metadata)
# print("Ingest complete:", ok)

# # process watch history using actual runtimes
# sqlite._INIT_POPULATE_watch_hist_raw_events(Emby.get_all_watch_hist)
# sqlite._INIT_POPULATE_watch_hist_agg_sessions()
# sqlite._INIT_POPULATE_watch_hist_user_item_stats()
# sqlite.update_completion_ratios()

#  # create and ingest TMDB tables
# sqlite._INIT_create_tmdb_schemas()
# sqlite.ingest_tmdb_movie_tv_genres(TMDB.fetch_movie_genres, TMDB.fetch_tv_genres)


#---- testing ml pre-processing ----
pp = PreProcess()
# one hot encode each title with its genre tags
df = pp.imdb_get_encoded_genres(cache_path="data/cache/imdb_genres_ohe.parquet", refresh=False)
# print(len(df))
# print(df.head())

# extract OHE columns
genre_cols = [c for c in df.columns if c.startswith("genre_")]
X = df[genre_cols].values.astype(np.float32)
# normalise rows for cosine similarity
X = normalize(X, norm="l2")

# fit a k-NN model
knn = NearestNeighbors(metric="cosine", algorithm="brute")
knn.fit(X)

# ------------- title-based recommendations -> pick a title row, find its neighbours: "because you watched X, here are similar titles."
# e.g: first row
i = 100000
distances, indices = knn.kneighbors(X[i].reshape(1, -1), n_neighbors=6)

# indices[0] are the row IDs of nearest neighbours
# distances[0] are cosine distances (smaller = more similar)
for rank, j in enumerate(indices[0][1:], start=1):  # skip self
    print(f"{rank}. {df.loc[j, 'primary_name']} (distance={distances[0][rank]:.3f})")
    
    
# ------------- user watch history-based recommendations

# collect the set of titles they’ve watched from Emby.
# grab their vectors from X, build a weighted average user vector:
# watched_idx = [ ... ]  # indices of titles watched
# weights = [ ... ]      # e.g. completion ratio, minutes watched

# user_vec = np.average(X[watched_idx], axis=0, weights=weights)
# user_vec = user_vec / np.linalg.norm(user_vec)  # normalize

# # query the k-NN index with this user vector
# distances, indices = nn.kneighbors(user_vec.reshape(1, -1), n_neighbors=20)
