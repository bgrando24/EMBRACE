from ml import PreProcess
import numpy as np
from sklearn.preprocessing import normalize
from sklearn.neighbors import NearestNeighbors

#---- testing ml pre-processing ----
pp = PreProcess()
# one hot encode each title with its genre tags
df = pp.imdb_get_encoded_genres(cache_path="data/cache/imdb_genres_ohe.parquet", refresh=False)
# print(len(df))
# print(df.head())

# extract OHE columns
genre_cols = [c for c in df.columns if c.startswith("genre_")]
X = df[genre_cols].values.astype(np.float32)
print(f"Size of genre cols: {len(X)}")
# normalise rows for cosine similarity
X = normalize(X, norm="l2")

# fit a k-NN model
knn = NearestNeighbors(metric="cosine", algorithm="brute")
knn.fit(X)

# ------------- title-based recommendations -> pick a title row, find its neighbours: "because you watched X, here are similar titles."
# row index
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