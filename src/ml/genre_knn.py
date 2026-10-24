from typing import Iterable, Optional

import numpy as np
import pandas as pd
from sklearn.preprocessing import normalize


class GenreKNN:
    """
    Item-to-item recommender over multi-hot IMDB genre vectors, using cosine similarity.

    "Because you watched X": takes one title and returns the titles whose genre sets are most similar.
    `recommend_for_profile()` does the same for a blend of titles (a user's history, see `UserGenreProfiles`).

    ---
    Why not sklearn's NearestNeighbors?

    Brute-force cosine kNN on L2-normalised rows is just one dot product per title, which is what `recommend()` does.
    Doing it directly fixes two problems NearestNeighbors has with this data:

    1. Ties: genre-only vectors are very coarse (IMDB gives at most 3 genres per title), so e.g. every "Comedy"-only
       title has exactly the same similarity to a seed. `kneighbors(k=200)` returns an arbitrary 200 of those.
       Here ties are broken by a documented rule instead: similarity, then num_votes (more popular first), then t_const.
    2. Filtering: we usually only want titles on the Emby server. kNN picks the top k from ALL of IMDB first, which may
       contain none of them; here the filter is applied before ranking.

    The popularity tie-break is a deliberate choice: among titles with identical genres, it favours well-known ones.
    It is visible in the output (num_votes) so it isn't mistaken for genre similarity.
    """

    OUTPUT_COLS = ["t_const", "primary_name", "type", "start_year", "avg_rating", "num_votes"]

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
        self.titles = titles.loc[has_genre].reset_index(drop=True)
        if self.titles["t_const"].duplicated().any():
            raise ValueError("titles must have one row per t_const")

        self._row_of = pd.Series(self.titles.index, index=self.titles["t_const"])
        self._genres = self.titles[self.genre_cols].to_numpy(dtype=bool)
        # L2-normalise each row, so the dot product of two rows is their cosine similarity
        self._X = normalize(self._genres.astype(np.float32), norm="l2")
        self._votes = self.titles["num_votes"].fillna(0).to_numpy(dtype=np.int64)
        self._ids = self.titles["t_const"].to_numpy(dtype=str)
        self._types = self.titles["type"].to_numpy(dtype=str)

    def __contains__(self, t_const: str) -> bool:
        return t_const in self._row_of.index

    def recommend(
        self,
        t_const: str,
        n: int = 10,
        allowed: Optional[Iterable[str]] = None,
        types: Optional[Iterable[str]] = None,
    ) -> pd.DataFrame:
        """
        Returns the `n` titles most similar to `t_const` by genre, never including the seed itself.

        Args:
            t_const (str): IMDB ID of the seed title, e.g. "tt4647692"
            n (int, optional): Number of recommendations - DEFAULT: 10
            allowed (Iterable[str], optional): Only recommend these IMDB IDs (e.g. titles on the Emby server)
            types (Iterable[str], optional): Only recommend these IMDB title types (e.g. ["tvSeries"])

        Returns:
            DataFrame: OUTPUT_COLS + `similarity` (cosine, 0-1) + `shared_genres` (genres in common with the seed),
            ordered by similarity, then num_votes, then t_const

        Raises:
            KeyError: if the seed isn't in the model (unknown ID, filtered title type, or no genres)
        """
        if t_const not in self:
            raise KeyError(f"{t_const} is not in the model (unknown, filtered out by title type, or has no genres)")
        seed = int(self._row_of[t_const])

        similarity = self._X @ self._X[seed]
        mask = self._filter_mask(allowed, types)
        mask[seed] = False
        top, similarity = self._rank(similarity, mask, n)

        out = self.titles.loc[top, self.OUTPUT_COLS].copy()
        out["similarity"] = similarity[top]
        seed_genres = self._genres[seed]
        out["shared_genres"] = [
            [c.removeprefix("genre_") for c, shared in zip(self.genre_cols, self._genres[j] & seed_genres) if shared]
            for j in top
        ]
        return out.reset_index(drop=True)

    def profile_vector(self, t_consts: Iterable[str], weights: Iterable[float]) -> np.ndarray:
        """
        A taste profile in the same space as the titles: the weighted sum of the titles' (normalised) genre vectors,
        L2-normalised so `recommend_for_profile()` scores titles by cosine similarity to it.

        Each title contributes the same total amount however many genres it has, so a 3-genre title doesn't count
        more than a 1-genre one. Titles not in the model are skipped.

        Raises:
            ValueError: if no title with a positive weight is in the model
        """
        weights = pd.Series(list(weights), index=list(t_consts), dtype=float)
        weights = weights[weights.index.isin(self._row_of.index) & (weights > 0)]
        if weights.empty:
            raise ValueError("None of the weighted titles are in the model")
        vector = weights.to_numpy(dtype=np.float32) @ self._X[self._row_of[weights.index].to_numpy()]
        return vector / np.linalg.norm(vector)

    def recommend_for_profile(
        self,
        profile: np.ndarray,
        n: int = 10,
        allowed: Optional[Iterable[str]] = None,
        types: Optional[Iterable[str]] = None,
        exclude: Optional[Iterable[str]] = None,
    ) -> pd.DataFrame:
        """
        Returns the `n` titles closest to a taste profile (see `profile_vector()`), ranked like `recommend()`.

        Args:
            profile (ndarray): output of `profile_vector()`
            n, allowed, types: as for `recommend()`
            exclude (Iterable[str], optional): never recommend these IMDB IDs, e.g. titles the user already watched

        Returns:
            DataFrame: OUTPUT_COLS + `similarity` (cosine, 0-1) + `genres` (the title's genres)
        """
        similarity = self._X @ profile.astype(np.float32)
        mask = self._filter_mask(allowed, types)
        if exclude is not None:
            mask &= ~np.isin(self._ids, np.fromiter(exclude, dtype=object).astype(str))
        top, similarity = self._rank(similarity, mask, n)

        out = self.titles.loc[top, self.OUTPUT_COLS].copy()
        out["similarity"] = similarity[top]
        out["genres"] = [self.genres_of(self._ids[j]) for j in top]
        return out.reset_index(drop=True)

    def _filter_mask(self, allowed: Optional[Iterable[str]], types: Optional[Iterable[str]]) -> np.ndarray:
        """Boolean mask over the model's rows of the titles that may be recommended"""
        mask = np.ones(len(self.titles), dtype=bool)
        if allowed is not None:
            mask &= np.isin(self._ids, np.fromiter(allowed, dtype=object).astype(str))
        if types is not None:
            mask &= np.isin(self._types, np.array(list(types), dtype=str))
        return mask

    def _rank(self, similarity: np.ndarray, mask: np.ndarray, n: int) -> tuple[np.ndarray, np.ndarray]:
        """
        Row indices of the top `n` masked titles, ordered by similarity desc, then num_votes desc, then t_const asc.
        Also returns the rounded similarity array the ranking used.
        """
        # round so float noise doesn't break exact ties (equal genre sets must compare equal)
        similarity = np.round(similarity, 6)
        candidates = np.flatnonzero(mask)
        if len(candidates) == 0:
            return candidates, similarity

        # only fully sort titles that can make the top n: everything at or above the n-th best similarity (incl. ties)
        if len(candidates) > n:
            nth_best = np.partition(similarity[candidates], -n)[-n]
            candidates = candidates[similarity[candidates] >= nth_best]
        # np.lexsort sorts by the LAST key first: similarity desc, then votes desc, then t_const asc
        order = np.lexsort((self._ids[candidates], -self._votes[candidates], -similarity[candidates]))
        return candidates[order[:n]], similarity

    def genres_of(self, t_const: str) -> list[str]:
        """Genres of a title in the model"""
        row = self._genres[int(self._row_of[t_const])]
        return [c.removeprefix("genre_") for c, has in zip(self.genre_cols, row) if has]
