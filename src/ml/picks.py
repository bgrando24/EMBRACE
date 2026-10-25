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
FRESH = "fresh"
BACK_CATALOGUE = "back_catalogue"

# users with fewer profile titles than this get no picks: a handful of titles is noise, not taste
MIN_PROFILE_TITLES = 5

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

    @classmethod
    def load(cls, pp: PreProcess, log: Callable[[str], None] = print) -> "Recommender":
        """
        Builds the default model: ContentKNN (with MovieLens when downloaded), falling back to GenreKNN when no TMDB
        details have been fetched yet. Profiles include titles Emby has marked played.
        """
        titles = pp.imdb_get_encoded_genres()
        try:
            movielens = pp.get_movielens_likers()
        except FileNotFoundError:
            log("MovieLens not downloaded (scripts/movielens/download_movielens.sh), its block stays empty")
            movielens = None
        content = None
        try:
            content = pp.get_title_content(titles, movielens)
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
        )

    def eligible_users(self, min_titles: int = MIN_PROFILE_TITLES) -> pd.DataFrame:
        """`UserProfiles.users()` rows with at least `min_titles` titles in their profile"""
        users = self.profiles.users()
        return users[users["titles_in_profile"] >= min_titles].reset_index(drop=True)

    def picks(self, user_id: str, n: int = 10, when=None) -> pd.DataFrame:
        """
        The user's two lists, fresh first. A title is in at most one of them.

        Args:
            user_id (str): canonical user ID
            n (int, optional): titles per list - DEFAULT: 10
            when (optional): the time to rank at (freshness + requests made before it) - DEFAULT: now

        Returns:
            DataFrame: PICK_COLS. `rank` is 1-based within each list; `score` is similarity + boost (fresh) or the
            similarity (back catalogue); `added_days_ago` / `you_requested` describe fresh picks (NaN / False otherwise)
        """
        added = self.freshness.at(when)
        lists = []
        if len(added):     # no dates until a refresh has stored Emby's DateCreated
            mine = self.requests.boost(when, user_id=user_id)
            fresh = self.profiles.recommend(user_id, n=n, allowed=self.library, boost=self.freshness.boost(when).add(mine, fill_value=0))
            fresh["added_days_ago"] = fresh["t_const"].map(added["age_days"]).round(0)
            fresh["you_requested"] = fresh["t_const"].isin(mine.index)
            lists.append(fresh.assign(list=FRESH))
        taken = set(lists[0]["t_const"]) if lists else set()
        back = self.profiles.recommend(user_id, n=n, allowed=self.library - taken)
        lists.append(back.assign(list=BACK_CATALOGUE, score=back["similarity"], added_days_ago=float("nan"), you_requested=False))

        out = pd.concat(lists, ignore_index=True)
        out["rank"] = out.groupby("list").cumcount() + 1
        return out.reindex(columns=PICK_COLS)

    def all_picks(self, n: int = 10, when=None, min_titles: int = MIN_PROFILE_TITLES,
                  users: Optional[set[str]] = None) -> pd.DataFrame:
        """
        `picks()` for every eligible user, with `user_id` + `user_name` columns.

        Args:
            users (set, optional): only these user IDs (still subject to `min_titles`)
        """
        eligible = self.eligible_users(min_titles)
        if users is not None:
            eligible = eligible[eligible["user_id"].isin(users)]
        frames = [
            self.picks(user_id, n=n, when=when).assign(user_id=user_id, user_name=user_name)
            for user_id, user_name in eligible[["user_id", "user_name"]].itertuples(index=False)
        ]
        if not frames:
            return pd.DataFrame(columns=["user_id", "user_name"] + PICK_COLS)
        out = pd.concat(frames, ignore_index=True)
        return out[["user_id", "user_name"] + PICK_COLS]
