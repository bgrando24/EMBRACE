import pandas as pd


class Requests:
    """
    Jellyseerr requests as a ranking signal: boost titles a user asked for (and, optionally, titles anyone asked for)
    once they're on the server. Built from `PreProcess.get_user_requests()`.

    Chosen with the time-aware evaluation (`python -m ml.evaluate --temporal`, Oct 2026, 195 requests): 25 of 133
    held-out titles had been requested by that user beforehand. On top of the freshness boost, +0.1 for the user's own
    requests moves those from a median rank of 4 to 1: MRR 0.202 -> 0.238, AUC 0.822 -> 0.832, with a plateau from
    0.05 to 0.15. Boosting other people's requests made things worse, so `others` is 0. Requests are NOT used in
    profiles: only ~1 in 4 gets watched by its requester, and adding them lowered both evaluations.
    """

    DEFAULT_OWN = 0.1

    def __init__(self, requests: pd.DataFrame):
        r = requests.dropna(subset=["t_const", "requested_at"])
        self._requests = r.assign(requested_at=pd.to_datetime(r["requested_at"]))

    def titles(self) -> set[str]:
        """IMDB IDs of every title anyone has requested"""
        return set(self._requests["t_const"])

    def boost(self, when=None, user_id=None, own: float = DEFAULT_OWN, others: float = 0.0) -> pd.Series:
        """
        t_const -> `own` for titles `user_id` requested before `when` (DEFAULT: now), else `others` for titles
        someone else requested. To add to a similarity (see `CosineKNN.recommend_for_profile(boost=...)`).
        """
        r = self._requests
        if when is not None:
            r = r[r["requested_at"] < pd.Timestamp(when)]
        mine = set(r.loc[r["user_id"] == user_id, "t_const"]) if user_id is not None else set()
        out = pd.Series(others, index=pd.Index(sorted(set(r["t_const"])), name="t_const"), dtype=float)
        out[out.index.isin(mine)] = own
        return out[out > 0]
