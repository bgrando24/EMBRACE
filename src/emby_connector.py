from typing import Final
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

        
    def ping_server(self): 
        ping_res = requests.get(f"{self.__BASE_DOMAIN}/System/Ping?api_key={self.__EMBY_API_KEY}")
        if not ping_res.ok:
            if self._debug: print(f"Error: Emby server at {self.__BASE_DOMAIN} is unavailable. Status code: {ping_res.status_code}", file=sys.stderr)
            exit(1)
        else:
            if self._debug: print(f"Emby server available at [{self.__BASE_DOMAIN}]! Status: {ping_res.status_code}")
    
    
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
            
        