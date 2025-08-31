import requests
from typing import Final, Dict
import sys

from custom_types import T_TMDBGenres

class TMDBConnector:
    """Provides functionality for interfacing with the TMDB API service"""
    
    def __init__(self, TMDB_READ_ACCESS_TOKEN, debug = False):
        """
        Args:
            TMDB_READ_ACCESS_TOKEN (str): Your TMDB account's "read access token" - different from your account's "API key"
            debug (bool): Optional, whether to display debug print statements
        """
        
        self._debug: Final = debug
        
        if TMDB_READ_ACCESS_TOKEN is None:
            print("Error: EMBY_API_KEY environment variable is not set.", file=sys.stderr)
            exit(1)
        
        self.__BASE_DOMAIN = "https://api.themoviedb.org/3"
        self.__API_READ_TOKEN: Final = TMDB_READ_ACCESS_TOKEN
        
    
    def fetch_movie_genres(self) -> T_TMDBGenres:
        """
        Fetches all of TMDB's movie genres
        
        Returns: dict[int, str]: key 'id' = integer ID for the genre, value 'name' = string text of the genre
        """
        
        if self._debug: print(f"[TMDB Connector] - Attempting to fetch: {self.__BASE_DOMAIN}/genres/movie/list")
        
        return requests.get(
            f"{self.__BASE_DOMAIN}/genre/movie/list", 
            {"language": "en"},
            headers={"Authorization": f"Bearer {self.__API_READ_TOKEN}"}
            ).json()['genres']
    
    
    def fetch_tv_genres(self) -> T_TMDBGenres:
        """
        Fetches all of TMDB's tv genres
        
        Returns: dict[int, str]: key 'id' = integer ID for the genre, value 'name' = string text of the genre
        """
        
        if self._debug: print(f"[TMDB Connector] - Attempting to fetch: {self.__BASE_DOMAIN}/genres/tv/list")
        
        return requests.get(
            f"{self.__BASE_DOMAIN}/genre/tv/list", 
            {"language": "en"}, 
            headers={"Authorization": f"Bearer {self.__API_READ_TOKEN}"}
            ).json()['genres']
        