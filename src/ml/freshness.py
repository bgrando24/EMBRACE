import numpy as np
import pandas as pd


class Freshness:
    """
    How recently each title arrived on the Emby server, as of any date: for a movie when it was added, for a series
    when its newest episode (or the series itself) was added - a show getting new episodes counts as fresh again.

    Built from Emby's DateCreated (`library_items.date_created`). Caveats, as of Oct 2026:
    - Removed items have no date (it was only stored from Oct 2026 on), so titles only known from the old server
      are never "fresh" and never count as available at a past date.
    - The server rebuild re-imported ~840 episodes on 2026-07-24/25, so some shows look like they got new episodes
      then. Movies keep their file dates.
    """

    # chosen with the time-aware evaluation (`python -m ml.evaluate --temporal`, Oct 2026: 133 held out): "newest
    # first" alone is the strongest single signal (hit@10 0.38 vs 0.05 for content), and content + this boost is best
    # overall (AUC 0.82). Results plateau for weights 0.2-1.0 and half-lives 14-60 days; these are mid-plateau.
    DEFAULT_WEIGHT = 0.3
    DEFAULT_HALF_LIFE_DAYS = 30.0

    def __init__(self, additions: pd.DataFrame):
        """
        Args:
            additions (DataFrame): output of `PreProcess.get_title_additions()` - one row per Emby item with
                `t_const` (the IMDB title it counts towards) and `added_at` (timestamp)
        """
        a = additions.dropna(subset=["t_const", "added_at"])
        self._added = pd.to_datetime(a["added_at"], utc=True).dt.tz_localize(None)
        self._t_const = a["t_const"].to_numpy()

    def at(self, when=None) -> pd.DataFrame:
        """
        Titles on the server at `when` (DEFAULT: now), with `first_added`, `last_added` (newest item added up to
        `when`) and `age_days` (days since last_added). Indexed by t_const.
        """
        when = pd.Timestamp.now() if when is None else pd.Timestamp(when)
        when = when.tz_localize(None) if when.tzinfo else when
        known = (self._added <= when).to_numpy()
        df = pd.DataFrame({"t_const": self._t_const[known], "added_at": self._added.to_numpy()[known]})
        out = df.groupby("t_const")["added_at"].agg(first_added="min", last_added="max")
        out["age_days"] = (when - out["last_added"]).dt.total_seconds() / 86400
        return out

    def boost(self, when=None, weight: float = DEFAULT_WEIGHT, half_life_days: float = DEFAULT_HALF_LIFE_DAYS) -> pd.Series:
        """
        t_const -> weight * 0.5 ** (age_days / half_life_days): `weight` for a title added today, half that after
        `half_life_days`. To add to a cosine similarity (see `CosineKNN.recommend_for_profile(boost=...)`).

        With the defaults, a title added today gets +0.3 - more than almost any profile similarity (typically 0.1-0.3)
        - so new arrivals lead, ordered by taste, and a title's boost is down to +0.04 after 3 months.
        """
        age = self.at(when)["age_days"]
        return weight * np.power(0.5, age / half_life_days)
