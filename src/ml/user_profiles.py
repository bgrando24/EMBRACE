from typing import Iterable, Optional

import numpy as np
import pandas as pd

from .cosine_knn import CosineKNN


# a finished movie counts 1; this many hours of a series counts the same
MOVIE_EQUIVALENT_HOURS = 2.0
# titles below this interest are left out of the profile (opened and abandoned), but are still never recommended
MIN_INTEREST = 0.1


def title_interest(user_titles: pd.DataFrame) -> pd.Series:
    """
    How much each (user, title) row says about the user's taste, from how much of it they watched.

    - Movie: the fraction of the runtime watched, in total across all sessions, capped at 1 (a rewatch doesn't count
      double). Per-session completion isn't used, because a movie watched over three evenings is three "partial"
      sessions. Unknown runtime falls back to MOVIE_EQUIVALENT_HOURS.
    - Series: log2(1 + hours / MOVIE_EQUIVALENT_HOURS) - 2 hours counts like one finished movie, 6 hours like 2,
      14 hours like 3. The log keeps one long binge from drowning out everything else.

    Args:
        user_titles (DataFrame): output of `PreProcess.get_user_title_stats()[0]`

    Returns:
        Series: interest >= 0, aligned to user_titles' index
    """
    hours = user_titles["total_seconds_watched"].astype(float) / 3600
    runtime_hours = (user_titles["runtime_seconds"].astype(float) / 3600).where(lambda h: h > 0, MOVIE_EQUIVALENT_HOURS)
    movie = (hours / runtime_hours).clip(upper=1.0)
    series = np.log2(1 + hours / MOVIE_EQUIVALENT_HOURS)
    return movie.where(user_titles["unit_type"] == "Movie", series)


class UserProfiles:
    """
    Per-user recommendations from watch history: each user's taste profile is the interest-weighted blend of the feature
    vectors of everything they watched (see `title_interest()`), and recommendations are the titles closest to it.
    Works with any `CosineKNN` model (`GenreKNN`, `ContentKNN`).
    """

    def __init__(self, knn: CosineKNN, user_titles: pd.DataFrame, min_interest: float = MIN_INTEREST):
        """
        Args:
            knn (CosineKNN): the recommender model, e.g. GenreKNN or ContentKNN
            user_titles (DataFrame): output of `PreProcess.get_user_title_stats()[0]`
            min_interest (float, optional): titles below this are left out of the profile - DEFAULT: MIN_INTEREST
        """
        self.knn = knn
        self.min_interest = min_interest
        self.history = user_titles.assign(
            interest=title_interest(user_titles),
            in_model=user_titles["imdb_id"].map(lambda t: t in knn),
        )

    def users(self) -> pd.DataFrame:
        """One row per user: titles watched, how many shape the profile, and hours watched"""
        h = self.history.assign(in_profile=self._in_profile(self.history), hours=self.history["total_seconds_watched"] / 3600)
        return (
            h.groupby(["user_id", "user_name"], as_index=False, dropna=False)
            .agg(titles_watched=("imdb_id", "nunique"), titles_in_profile=("in_profile", "sum"), hours_watched=("hours", "sum"))
            .sort_values("hours_watched", ascending=False, ignore_index=True)
        )

    def profile_titles(self, user_id: str) -> pd.DataFrame:
        """The user's titles that shape their profile, highest interest first"""
        h = self._history_of(user_id)
        h = h[self._in_profile(h)]
        cols = ["imdb_id", "unit_name", "unit_type", "items_watched", "total_seconds_watched", "interest"]
        return h.sort_values(["interest", "imdb_id"], ascending=[False, True])[cols].reset_index(drop=True)

    def genre_shares(self, user_id: str) -> pd.Series:
        """
        Readable summary of the profile: each title's interest split equally between its genres, as a share of the
        total. Sums to 1, highest first.
        """
        titles = self.profile_titles(user_id)
        shares: dict[str, float] = {}
        for t_const, interest in zip(titles["imdb_id"], titles["interest"]):
            genres = self.knn.genres_of(t_const)
            for g in genres:
                shares[g] = shares.get(g, 0.0) + interest / len(genres)
        s = pd.Series(shares, dtype=float)
        return (s / s.sum()).sort_values(ascending=False) if not s.empty else s

    def recommend(
        self,
        user_id: str,
        n: int = 10,
        allowed: Optional[Iterable[str]] = None,
        types: Optional[Iterable[str]] = None,
        boost: Optional[pd.Series] = None,
    ) -> pd.DataFrame:
        """
        The `n` titles closest to the user's taste profile that they haven't watched (or opened and abandoned).

        Args:
            user_id (str): canonical Emby user ID (aliases are already resolved in `get_user_title_stats()`)
            n, allowed, types: as for `GenreKNN.recommend()`
            boost (Series, optional): t_const -> amount added to the similarity before ranking, e.g.
                `Freshness.boost()` to put new arrivals first (see `CosineKNN.recommend_for_profile()`)

        Returns:
            DataFrame: `recommend_for_profile()` columns + `because_you_watched` (the most similar profile title, ties
            going to the one with the most interest; None if nothing is shared) + `shared` (what the two have in common)

        Raises:
            KeyError: unknown user
            ValueError: none of the user's titles are in the model with enough interest
        """
        titles = self.profile_titles(user_id)
        profile = self.knn.profile_vector(titles["imdb_id"], titles["interest"])
        recs = self.knn.recommend_for_profile(
            profile, n=n, allowed=allowed, types=types, exclude=self._history_of(user_id)["imdb_id"], boost=boost
        )
        if recs.empty:
            return recs.assign(because_you_watched=pd.Series(dtype=object), shared=pd.Series(dtype=object))

        # similarity first, not interest x similarity: otherwise one long binge "explains" anything sharing a genre
        # with it. Titles are sorted by interest, and argmax returns the first maximum, so ties go to higher interest.
        similarity = np.round(self.knn.similarity_between(titles["imdb_id"], recs["t_const"]), 6)
        best = similarity.argmax(axis=0)
        has_reason = similarity.max(axis=0) > 0
        recs["because_you_watched"] = np.where(has_reason, titles["unit_name"].to_numpy()[best], None)
        recs["shared"] = [
            self.knn.shared_between(titles["imdb_id"].iloc[i], [rec])[0] if ok else []
            for i, rec, ok in zip(best, recs["t_const"], has_reason)
        ]
        return recs

    def _history_of(self, user_id: str) -> pd.DataFrame:
        h = self.history[self.history["user_id"] == user_id]
        if h.empty:
            raise KeyError(f"No watch history for user {user_id}")
        return h

    def _in_profile(self, h: pd.DataFrame) -> pd.Series:
        return h["in_model"] & (h["interest"] >= self.min_interest)
