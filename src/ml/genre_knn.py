import numpy as np
import pandas as pd

from .cosine_knn import CosineKNN


class GenreKNN(CosineKNN):
    """
    Recommender over multi-hot IMDB genre vectors, using cosine similarity (see `CosineKNN` for ranking and ties).

    "Because you watched X": takes one title and returns the titles whose genre sets are most similar.
    `recommend_for_profile()` does the same for a blend of titles (a user's history, see `UserProfiles`).
    Covers all of IMDB, but genres are coarse; `ContentKNN` adds keywords, people and descriptions for library titles.
    """

    SHARED_COL = "shared_genres"

    def __init__(self, titles: pd.DataFrame):
        """
        Args:
            titles (DataFrame): output of `PreProcess.imdb_get_encoded_genres()` - one row per title with
                `t_const`, `primary_name`, `type`, `start_year`, `avg_rating`, `num_votes` and `genre_*` 0/1 columns
        """
        self.genre_cols: list[str] = sorted(c for c in titles.columns if c.startswith("genre_"))
        if not self.genre_cols:
            raise ValueError("No genre_* columns found in titles DataFrame")

        # a title with no genres is a zero vector: it has no direction, so cosine similarity is undefined for it
        has_genre = titles[self.genre_cols].to_numpy().sum(axis=1) > 0
        titles = titles.loc[has_genre].reset_index(drop=True)
        self._genres = titles[self.genre_cols].to_numpy(dtype=bool)
        self._init_index(titles, self._genres.astype(np.float32))

    def genres_of(self, t_const: str) -> list[str]:
        """Genres of a title in the model"""
        row = self._genres[int(self._row_of[t_const])]
        return [c.removeprefix("genre_") for c, has in zip(self.genre_cols, row) if has]

    def _shared(self, seed: int, rows: np.ndarray) -> list[list[str]]:
        seed_genres = self._genres[seed]
        return [
            [c.removeprefix("genre_") for c, shared in zip(self.genre_cols, self._genres[j] & seed_genres) if shared]
            for j in rows
        ]
