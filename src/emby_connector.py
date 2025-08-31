from typing import Final, Dict, Iterable, Optional, Sequence, Tuple
import requests
import sys

from custom_types import T_EmbyUserWatchHistResponse, T_EmbyUsersResponse, T_EmbyAllUserWatchHist

class EmbyConnector:
    """
    Provides functionality for communicating with the Emby API
    """
    
    # TODO: add logic to check for existing DB
    def __init__(self, BASE_DOMAIN, EMBY_API_KEY, debug = False):
        """
        Args:
            BASE_DOMAIN (str):  Base URL for the Emby server, EXPECTED FORMAT: https://[domain]/emby
            EMBY_API_KEY (str): Your Emby server API key
            debug (bool): Optional, whether to display debug print statements
        """
        
        self._debug: Final = debug
        
        # exit if variables are unavailable
        if BASE_DOMAIN is None:
            print("Error: BASE_DOMAIN environment variable is not set.", file=sys.stderr)
            exit(1)
        if EMBY_API_KEY is None:
            print("Error: EMBY_API_KEY environment variable is not set.", file=sys.stderr)
            exit(1)
        
        self.__BASE_DOMAIN: Final   = BASE_DOMAIN
        self.__EMBY_API_KEY: Final  = EMBY_API_KEY
        
    
    def _default_item_fields(self) -> str:
        """
        The field list to request on /Items: https://dev.emby.media/doc/restapi/Item-Information.html
        """
        fields: Sequence[str] = [
            "ParentId","ProviderIds","Path","RunTimeTicks","PremiereDate",
            "ProductionYear","SeriesId","SeriesName","SeasonId",
            "IndexNumber","ParentIndexNumber","Overview","CommunityRating",
            "Container","Width","Height","MediaStreams","GenreItems","TagItems"
        ]
        return ",".join(fields)

        
    def ping_server(self): 
        ping_res = requests.get(f"{self.__BASE_DOMAIN}/System/Ping?api_key={self.__EMBY_API_KEY}")
        if not ping_res.ok:
            if self._debug: print(f"Error: Emby server at {self.__BASE_DOMAIN} is unavailable. Status code: {ping_res.status_code}", file=sys.stderr)
            exit(1)
        else:
            if self._debug: print(f"Emby server available at [{self.__BASE_DOMAIN}]! Status: {ping_res.status_code}")
    
    
    def get_default_user_id(self) -> str:
        """
        Get the first available user ID (you might want to make this configurable)
        """
        users = self.get_all_emby_users()
        if users:
            return list(users.values())[0]
        raise Exception("No users found")
    
    
    def get_media_folders(self) -> Dict[str, str]:
        """
        Returns a dict {folder_name: folder_id} for top-level media folders.
        """
        url = f"{self.__BASE_DOMAIN}/Library/MediaFolders"
        r = requests.get(url, params={"api_key": self.__EMBY_API_KEY}, timeout=30)
        r.raise_for_status()
        data = r.json()
        out = {}
        for f in data.get("Items", []):
            # f example: {"Name": "movies", "Id": "5", "CollectionType": "movies", ...}
            out[f.get("Name") or f.get("CollectionType") or f.get("Id")] = f.get("Id")
        return out
            
    
    def get_item_metadata(self, item_id: str) -> dict:
        """
        Fetch detailed metadata for a specific item
        
        Args:
            item_id (str): The Emby item ID
        
        Returns:
            dict: Item metadata from Emby API
        """
        response = requests.get(
            f"{self.__BASE_DOMAIN}/Users/{self.get_default_user_id()}/Items/{item_id}?api_key={self.__EMBY_API_KEY}"
        )
        response.raise_for_status()
        return response.json()
        
    
    def get_all_emby_users(self) -> dict[str, str]:
        """
        Fetch all Emby users (including hidden users)

        Returns:
            dict[str, str]: key = username, value = user ID
        """
        response = requests.get(f"{self.__BASE_DOMAIN}/Users/Query?IsHidden=true&api_key={self.__EMBY_API_KEY}")
        response.raise_for_status()
        
        users_data: T_EmbyUsersResponse = response.json()
        users: dict[str, str] = {}
        
        for user in users_data["Items"]:
            users[user["Name"]] = user["Id"]
        return users


    def get_user_watch_hist(self, user_id: str, num_days: int | None = None, is_aggregated=False,) -> T_EmbyUserWatchHistResponse:
        """
        Fetch the watch history for a user
        Args:
            user_id (str): User's Emby ID
            num_days (int): The number of days of watch history to fetch
            is_aggregated (bool, optional): Whether to aggregate the data, defaults to False
        Returns:
            dict: The user's watch history data as a JSON-decoded dictionary
        """
        return requests.get(f"{self.__BASE_DOMAIN}/user_usage_stats/UserPlaylist?user_id={user_id}&aggregate_data={is_aggregated}&days={num_days}&api_key={self.__EMBY_API_KEY}").json()
    
    
    def get_all_watch_hist(self, num_days: int, is_aggregated=False) -> T_EmbyAllUserWatchHist:
        """
        Fetch watch history for all Emby users. Uses get_all_emby_users()
        Args:
            num_days (int): The number of days of watch history to fetch
            is_aggregated (bool, optional): Whether to aggregate the data, defaults to False
        Returns:
            dict: Keys = username, values = T_EmbyUserWatchHistResponse for that user
        """
        # get all users, loop through and fetch watch history for each user, build dictionary of results
        users = self.get_all_emby_users()
        all_watch_hist: T_EmbyAllUserWatchHist = {}
        for username, user_id in users.items():
            watch_data = self.get_user_watch_hist(user_id, num_days)
            all_watch_hist[username] = watch_data
        
        return all_watch_hist
    
    
    def get_items_page(
    self,
    start_index: int = 0,
    limit: int = 1000,
    include_item_types: Tuple[str, ...] = ("Movie", "Episode"),
    parent_id: Optional[str] = None,
    fields: Optional[str] = None,
    recursive: bool = True,
) -> dict:
        """
        Fetch a single page from Emby /Items.
        Returns the parsed JSON dict (QueryResult<BaseItemDto>) with Items[] and TotalRecordCount.
        """
        url = f"{self.__BASE_DOMAIN}/Items"
        params = {
            "IncludeItemTypes": ",".join(include_item_types),
            "Recursive": "true" if recursive else "false",
            "Fields": fields or self._default_item_fields(),
            "StartIndex": start_index,
            "Limit": limit,
            "api_key": self.__EMBY_API_KEY,
        }
        if parent_id:
            params["ParentId"] = parent_id

        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        return r.json()


    def iter_all_items(
        self,
        include_item_types: Tuple[str, ...] = ("Movie", "Episode"),
        parent_id: Optional[str] = None,
        page_size: int = 1000,
        fields: Optional[str] = None,
        recursive: bool = True,
    ) -> Iterable[dict]:
        """
        Yields every item (Movie + Episode by default) across the library, paging until complete.
        """
        start = 0
        total = None

        while True:
            page = self.get_items_page(
                start_index=start,
                limit=page_size,
                include_item_types=include_item_types,
                parent_id=parent_id,
                fields=fields,
                recursive=recursive,
            )
            items = page.get("Items", [])
            if total is None:
                total = page.get("TotalRecordCount", 0)

            for it in items:
                yield it

            got = len(items)
            if got == 0 or (start + got) >= (total or 0):
                break
            start += got
            
        