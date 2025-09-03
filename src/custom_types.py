from typing import TypedDict, List, Tuple

class T_EmbyUserWatchHistItem(TypedDict):
    """Single item in user's watch history from Emby API"""
    date: str            # yyyy-MM-dd 
    time: str            # 04:17:06
    user_id: str         # User's Emby ID
    item_name: str       # Letterkenny - s01e02 - Super Soft Birthday
    item_id: int         # 528946
    item_type: str       # "Episode", "Movie"
    duration: str        # in seconds: e.g. 25 minutes = (approx) 1563 seconds
    remote_address: str  # Subnet addr of device used by the user
    user_name: str       # Emby username
    user_has_image: bool # Whether user has profile image

#: Full response type from user watch history endpoint: /user_usage_stats/UserPlaylist
T_EmbyUserWatchHistResponse = List[T_EmbyUserWatchHistItem]

T_EmbyAllUserWatchHist = dict[str, T_EmbyUserWatchHistResponse]


T_EmbyWatchHistStatsRow = Tuple[
    int,    # stat_id
    str,    # user_id
    str,    # item_id
    int,    # total_sessions
    int,    # total_seconds_watched
    float,  # total_minutes_watched
    float,  # best_completion_ratio
    float,  # average_completion_ratio
    int,    # rewatch_count
    str,    # first_watched_timestamp
    str,    # last_watched_timestamp
    float,  # days_between_first_last
    float,  # adherence_score
    int,    # completed_sessions
    int,    # partial_sessions
    int,    # abandoned_sessions
    int,    # sampled_sessions
    str     # last_updated_timestamp
]

class T_EmbyUser(TypedDict):
    """User data from Emby API"""
    Name: str
    Id: str

class T_EmbyUsersResponse(TypedDict):
    """Shortened response from /Users/Query endpoint"""
    Items: List[T_EmbyUser]
    
T_TMDBGenre = TypedDict("T_TMDBGenre", {"id": int, "name": str})
T_TMDBGenres = list[T_TMDBGenre]