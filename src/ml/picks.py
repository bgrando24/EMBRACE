import functools
from dataclasses import dataclass
from typing import Callable, Optional

import pandas as pd

from .content_knn import ContentKNN
from .cosine_knn import CosineKNN
from .freshness import Freshness
from .genre_knn import GenreKNN
from .preprocess import PreProcess
from .requests_signal import Requests
from .user_profiles import UserProfiles


# the two lists each user gets (see docs/modules/ml.md#daily-picks):
# - fresh: new arrivals ranked by taste (+ the user's own Jellyseerr requests) - what people actually watch next
# - back_catalogue: taste alone, minus the fresh list - older titles Emby's own "Latest" row won't surface
# and, with discovery titles loaded, a third without a playlist (Emby playlists only hold what the server has):
# - to_request: taste alone over titles NOT on the server and not yet requested in Jellyseerr - worth requesting
FRESH = "fresh"
BACK_CATALOGUE = "back_catalogue"
TO_REQUEST = "to_request"
# titles to request must be well thought of: the titles beyond the server include TMDB's recommendations of library
# titles, some of them obscure or poorly rated (live run, Oct 2026: a 3.4 from 330 votes made the list)
REQUEST_MIN_RATING = 6.5
REQUEST_MIN_VOTES = 5_000

# users with fewer profile titles than this get no picks: a handful of titles is noise, not taste
MIN_PROFILE_TITLES = 5

# each list has at most one title per franchise (TMDB collection), and none this similar to a title above it
# (spin-offs, remakes, sequels without a collection) - see `diversify()`. In Oct 2026's lists only one pair repeated
# (two Conjuring films, 0.56); other titles in a list were 0.37 apart at most
NEAR_DUPLICATE_SIMILARITY = 0.5
# candidates ranked per list, as a multiple of its length, so titles left out for that are replaced by the next best
DIVERSITY_POOL = 3

def diversify(t_consts: list[str], knn: CosineKNN, collection: pd.Series, top: int) -> list[str]:
    """
    `t_consts` (best first) reordered so the first `top` have at most one title per franchise (`collection`:
    t_const -> TMDB collection name) and none NEAR_DUPLICATE_SIMILARITY or more similar to a title above it. The titles
    left out of the first `top` follow them, then the rest, each in their own order.
    """
    kept: list[str] = []
    later: list[str] = []
    franchises: set[str] = set()
    for t in t_consts:
        if len(kept) == top:
            break
        franchise = collection.get(t)
        franchise = franchise if isinstance(franchise, str) and franchise else None
        repeat = franchise in franchises
        if not repeat and t in knn:
            above = [k for k in kept if k in knn]
            repeat = bool(above) and knn.similarity_between([t], above).max() >= NEAR_DUPLICATE_SIMILARITY
        (later if repeat else kept).append(t)
        if franchise and not repeat:
            franchises.add(franchise)
    return kept + later + t_consts[len(kept) + len(later):]


PICK_COLS = ["list", "rank", "t_const", "primary_name", "start_year", "type", "similarity", "score",
             "because_you_watched", "shared", "added_days_ago", "you_requested"]


@dataclass
class Recommender:
    """Everything needed for per-user picks, loaded once: the model, profiles, library and the time-aware boosts"""

    knn: CosineKNN
    profiles: UserProfiles
    library: set[str]
    freshness: Freshness
    requests: Requests
    # `PreProcess.get_title_content()` rows (overviews, keywords, people) - None when the genre model was used
    content: Optional[pd.DataFrame] = None
    # the same for titles beyond the library (`load(discovery=True)`, for the chat-bot): never in picks
    discovery: Optional[pd.DataFrame] = None

    @classmethod
    def load(cls, pp: PreProcess, log: Callable[[str], None] = print, discovery: bool = False) -> "Recommender":
        """
        Builds the default model: ContentKNN (with MovieLens when downloaded), falling back to GenreKNN when no TMDB
        details have been fetched yet. Profiles include titles Emby has marked played. With `discovery`, the content
        of titles beyond the library is loaded too (as `discovery`); the model is the library's either way.
        """
        titles = pp.imdb_get_encoded_genres()
        try:
            movielens = pp.get_movielens_likers()
        except FileNotFoundError:
            log("MovieLens not downloaded (scripts/movielens/download_movielens.sh), its block stays empty")
            movielens = None
        content = extra = None
        try:
            content = pp.get_title_content(titles, movielens, discovery=discovery)
            library = content["in_library"].astype(bool)
            content, extra = content[library].reset_index(drop=True), (content[~library].reset_index(drop=True) if discovery else None)
            knn: CosineKNN = ContentKNN(content)
        except ValueError as e:
            log(f"Content model unavailable ({e}), using the genre model")
            knn = GenreKNN(titles)
        user_titles, _ = pp.get_user_title_stats()
        user_titles = pd.concat([user_titles, pp.get_played_titles(user_titles)], ignore_index=True)
        return cls(
            knn=knn,
            profiles=UserProfiles(knn, user_titles),
            library=pp.get_library_imdb_ids(),
            freshness=Freshness(pp.get_title_additions()),
            requests=Requests(pp.get_user_requests()),
            content=content,
            discovery=extra,
        )

    @functools.cached_property
    def collections(self) -> pd.Series:
        """t_const -> TMDB collection (franchise) name, for the library and the titles beyond it"""
        frames = [f[["t_const", "collection"]] for f in (self.content, self.discovery) if f is not None and "collection" in f]
        if not frames:
            return pd.Series(dtype=object)
        c = pd.concat(frames).dropna().drop_duplicates("t_const")
        return c.set_index("t_const")["collection"]

    def _diverse(self, recs: pd.DataFrame, n: int, knn: CosineKNN) -> pd.DataFrame:
        """The best `n` of `recs` after `diversify()`"""
        order = diversify(recs["t_const"].tolist(), knn, self.collections, n)[:n]
        return recs.set_index("t_const").loc[order].reset_index()

    @functools.cached_property
    def wide(self) -> tuple[CosineKNN, UserProfiles]:
        """
        The model and profiles over the library AND the titles beyond it (`discovery`), so the two can be compared:
        what the chat-bot searches, and what `to_request()` ranks. Built on first use (a few seconds, ~0.5 GB for
        ~12k titles). Without discovery titles, that's `knn` and `profiles`.
        """
        if self.discovery is None or not len(self.discovery):
            return self.knn, self.profiles
        knn = ContentKNN(pd.concat([self.content, self.discovery], ignore_index=True))
        return knn, UserProfiles(knn, self.profiles.history)

    def to_request(self, user_id: str, n: int = 5) -> pd.DataFrame:
        """
        The titles NOT on the server closest to the user's taste, leaving out any already requested in Jellyseerr (by
        anyone: they're on their way), any the user watched, and any rated below REQUEST_MIN_RATING on IMDB or with
        fewer than REQUEST_MIN_VOTES votes. Empty without discovery titles (`load(discovery=True)`).

        Returns:
            DataFrame: PICK_COLS, `list` TO_REQUEST, `score` the similarity
        """
        if self.discovery is None or not len(self.discovery):
            return pd.DataFrame(columns=PICK_COLS)
        knn, profiles = self.wide
        d = self.discovery
        good = (d["avg_rating"].fillna(0) >= REQUEST_MIN_RATING) & (d["num_votes"].fillna(0) >= REQUEST_MIN_VOTES)
        allowed = set(d.loc[good, "t_const"]) - self.library - self.requests.titles()
        out = self._diverse(profiles.recommend(user_id, n=n * DIVERSITY_POOL, allowed=allowed), n, knn)
        out = out.assign(list=TO_REQUEST, rank=range(1, len(out) + 1), score=out["similarity"],
                         added_days_ago=float("nan"), you_requested=False)
        return out.reindex(columns=PICK_COLS)

    def eligible_users(self, min_titles: int = MIN_PROFILE_TITLES) -> pd.DataFrame:
        """`UserProfiles.users()` rows with at least `min_titles` titles in their profile"""
        users = self.profiles.users()
        return users[users["titles_in_profile"] >= min_titles].reset_index(drop=True)

    def picks(self, user_id: str, n: int = 10, when=None, n_request: int = 0) -> pd.DataFrame:
        """
        The user's two lists, fresh first, then `n_request` titles to request (`to_request()`). A title is in at most
        one of them, and each list has at most one title per franchise and no near-duplicates (`diversify()`).

        Args:
            user_id (str): canonical user ID
            n (int, optional): titles per list - DEFAULT: 10
            when (optional): the time to rank at (freshness + requests made before it) - DEFAULT: now
            n_request (int, optional): titles beyond the server - DEFAULT: none

        Returns:
            DataFrame: PICK_COLS. `rank` is 1-based within each list; `score` is similarity + boost (fresh) or the
            similarity (back catalogue); `added_days_ago` / `you_requested` describe fresh picks (NaN / False otherwise)
        """
        added = self.freshness.at(when)
        lists = []
        if len(added):     # no dates until a refresh has stored Emby's DateCreated
            mine = self.requests.boost(when, user_id=user_id)
            fresh = self.profiles.recommend(user_id, n=n * DIVERSITY_POOL, allowed=self.library,
                                            boost=self.freshness.boost(when).add(mine, fill_value=0))
            fresh = self._diverse(fresh, n, self.knn)
            fresh["added_days_ago"] = fresh["t_const"].map(added["age_days"]).round(0)
            fresh["you_requested"] = fresh["t_const"].isin(mine.index)
            lists.append(fresh.assign(list=FRESH))
        taken = set(lists[0]["t_const"]) if lists else set()
        back = self._diverse(self.profiles.recommend(user_id, n=n * DIVERSITY_POOL, allowed=self.library - taken), n, self.knn)
        lists.append(back.assign(list=BACK_CATALOGUE, score=back["similarity"], added_days_ago=float("nan"), you_requested=False))
        if n_request > 0:
            lists.append(self.to_request(user_id, n=n_request))

        out = pd.concat([lst for lst in lists if len(lst)] or lists, ignore_index=True)
        out["rank"] = out.groupby("list").cumcount() + 1
        return out.reindex(columns=PICK_COLS)

    def all_picks(self, n: int = 10, when=None, min_titles: int = MIN_PROFILE_TITLES,
                  users: Optional[set[str]] = None, n_request: int = 0) -> pd.DataFrame:
        """
        `picks()` for every eligible user, with `user_id` + `user_name` columns.

        Args:
            users (set, optional): only these user IDs (still subject to `min_titles`)
            n_request (int, optional): as for `picks()`
        """
        eligible = self.eligible_users(min_titles)
        if users is not None:
            eligible = eligible[eligible["user_id"].isin(users)]
        frames = [
            self.picks(user_id, n=n, when=when, n_request=n_request).assign(user_id=user_id, user_name=user_name)
            for user_id, user_name in eligible[["user_id", "user_name"]].itertuples(index=False)
        ]
        if not frames:
            return pd.DataFrame(columns=["user_id", "user_name"] + PICK_COLS)
        out = pd.concat(frames, ignore_index=True)
        return out[["user_id", "user_name"] + PICK_COLS]
