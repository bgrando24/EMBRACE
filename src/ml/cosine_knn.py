from typing import Iterable, Optional

import numpy as np
import pandas as pd
import scipy.sparse as sp
from sklearn.preprocessing import normalize


class CosineKNN:
    """
    Shared core of the cosine-similarity recommenders (`GenreKNN`, `ContentKNN`): one feature vector per title,
    recommendations ranked by cosine similarity with a deterministic tie-break.

    ---
    Why not sklearn's NearestNeighbors?

    Brute-force cosine kNN on L2-normalised rows is just one dot product per title, which is what `recommend()` does.
    Doing it directly fixes two problems NearestNeighbors has with this data:

    1. Ties: coarse features (e.g. genre-only vectors - IMDB gives at most 3 genres per title) make many titles exactly
       equally similar to a seed. `kneighbors(k=200)` returns an arbitrary 200 of those.
       Here ties are broken by a documented rule instead: similarity, then num_votes (more popular first), then t_const.
    2. Filtering: we usually only want titles on the Emby server. kNN picks the top k from ALL titles first, which may
       contain none of them; here the filter is applied before ranking.

    The popularity tie-break is a deliberate choice: among equally similar titles, it favours well-known ones.
    It is visible in the output (num_votes) so it isn't mistaken for similarity.

    Subclasses build the features and call `_init_index()`, and implement `genres_of()` and `_shared()`.
    """

    OUTPUT_COLS = ["t_const", "primary_name", "type", "start_year", "avg_rating", "num_votes"]
    # name of the column `recommend()` explains each recommendation with (what it has in common with the seed)
    SHARED_COL = "shared"

    def _init_index(self, titles: pd.DataFrame, X) -> None:
        """
        Args:
            titles (DataFrame): one row per title (OUTPUT_COLS at least), aligned with the rows of X
            X (ndarray | scipy sparse matrix): one feature row per title, every row non-zero
        """
        self.titles = titles.reset_index(drop=True)
        if self.titles["t_const"].duplicated().any():
            raise ValueError("titles must have one row per t_const")
        self._row_of = pd.Series(self.titles.index, index=self.titles["t_const"])
        # L2-normalise each row, so the dot product of two rows is their cosine similarity
        self._X = normalize(X.astype(np.float32), norm="l2")
        if sp.issparse(self._X):
            self._X = sp.csr_matrix(self._X)
        self._votes = self.titles["num_votes"].fillna(0).to_numpy(dtype=np.int64)
        self._ids = self.titles["t_const"].to_numpy(dtype=str)
        self._types = self.titles["type"].fillna("").to_numpy(dtype=str)

    def genres_of(self, t_const: str) -> list[str]:
        """Genres of a title in the model"""
        raise NotImplementedError

    def _shared(self, seed: int, rows: np.ndarray) -> list[list[str]]:
        """What each of `rows` has in common with row `seed`, most telling first"""
        raise NotImplementedError

    def shared_between(self, a: str, b: Iterable[str]) -> list[list[str]]:
        """What each title in `b` has in common with title `a` (all must be in the model)"""
        return self._shared(int(self._row_of[a]), self._row_of[list(b)].to_numpy())

    def similarity_between(self, a: Iterable[str], b: Iterable[str]) -> np.ndarray:
        """Cosine similarity of every title in `a` to every title in `b` (all must be in the model): len(a) x len(b)"""
        A = self._X[self._row_of[list(a)].to_numpy()]
        B = self._X[self._row_of[list(b)].to_numpy()]
        S = A @ B.T
        return S.toarray() if sp.issparse(S) else np.asarray(S)

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
            DataFrame: OUTPUT_COLS + `similarity` (cosine, 0-1) + SHARED_COL (what it has in common with the seed),
            ordered by similarity, then num_votes, then t_const

        Raises:
            KeyError: if the seed isn't in the model (unknown ID, filtered title type, or no features)
        """
        if t_const not in self:
            raise KeyError(f"{t_const} is not in the model (unknown, or has no features)")
        seed = int(self._row_of[t_const])

        similarity = self._dot(self._X[seed])
        mask = self._filter_mask(allowed, types)
        mask[seed] = False
        top, similarity = self._rank(similarity, mask, n)

        out = self.titles.loc[top, self.OUTPUT_COLS].copy()
        out["similarity"] = similarity[top]
        out[self.SHARED_COL] = self._shared(seed, top)
        return out.reset_index(drop=True)

    def profile_vector(self, t_consts: Iterable[str], weights: Iterable[float]) -> np.ndarray:
        """
        A taste profile in the same space as the titles: the weighted sum of the titles' (normalised) genre vectors,
        L2-normalised so `recommend_for_profile()` scores titles by cosine similarity to it.

        Each title contributes the same total amount however many genres it has, so a 3-genre title doesn't count
        more than a 1-genre one. Titles not in the model are skipped. A negative weight subtracts the title (e.g. one
        the user gave up on, see `UserProfiles`), so titles like it rank lower.

        Raises:
            ValueError: if no title with a positive weight is in the model
        """
        weights = pd.Series(list(weights), index=list(t_consts), dtype=float)
        weights = weights[weights.index.isin(self._row_of.index) & (weights != 0)]
        if not (weights > 0).any():
            raise ValueError("None of the weighted titles are in the model")
        vector = np.asarray(weights.to_numpy(dtype=np.float32) @ self._X[self._row_of[weights.index].to_numpy()]).ravel()
        norm = np.linalg.norm(vector)
        return vector / norm if norm > 0 else vector

    def similarity_to_profile(self, profile: np.ndarray) -> pd.Series:
        """Cosine similarity of every title in the model to a taste profile (see `profile_vector()`), by t_const"""
        return pd.Series(np.round(self._dot(profile), 6), index=self._ids)

    def recommend_for_profile(
        self,
        profile: np.ndarray,
        n: int = 10,
        allowed: Optional[Iterable[str]] = None,
        types: Optional[Iterable[str]] = None,
        exclude: Optional[Iterable[str]] = None,
        boost: Optional[pd.Series] = None,
    ) -> pd.DataFrame:
        """
        Returns the `n` titles closest to a taste profile (see `profile_vector()`), ranked like `recommend()`.

        Args:
            profile (ndarray): output of `profile_vector()`
            n, allowed, types: as for `recommend()`
            exclude (Iterable[str], optional): never recommend these IMDB IDs, e.g. titles the user already watched
            boost (Series, optional): t_const -> amount added to the similarity before ranking (e.g. a freshness
                boost, see `Freshness.boost()`); titles not in it get 0

        Returns:
            DataFrame: OUTPUT_COLS + `similarity` (cosine, 0-1) + `genres` (the title's genres)
            + `boost` and `score` (similarity + boost, what was ranked on) when a boost is given
        """
        similarity = self._dot(profile)
        extra = np.zeros(len(self.titles), dtype=np.float64)
        if boost is not None:
            boost = boost[boost.index.isin(self._row_of.index)]
            extra[self._row_of[boost.index].to_numpy()] = boost.to_numpy(dtype=float)
        mask = self._filter_mask(allowed, types)
        if exclude is not None:
            mask &= ~np.isin(self._ids, np.fromiter(exclude, dtype=object).astype(str))
        top, score = self._rank(similarity + extra, mask, n)

        out = self.titles.loc[top, self.OUTPUT_COLS].copy()
        out["similarity"] = np.round(similarity[top], 6)
        out["genres"] = [self.genres_of(self._ids[j]) for j in top]
        if boost is not None:
            out["boost"] = np.round(extra[top], 6)
            out["score"] = score[top]
        return out.reset_index(drop=True)

    def _dot(self, vector) -> np.ndarray:
        """Dot product of every title's row with one vector (dense, or a 1-row sparse matrix)"""
        if sp.issparse(vector):
            vector = vector.toarray()
        return np.asarray(self._X @ np.asarray(vector, dtype=np.float32).ravel()).ravel()

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
