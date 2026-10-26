import requests
import time
from typing import Final, Dict, Literal, Optional
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
            print("Error: TMDB_READ_ACCESS_TOKEN environment variable is not set.", file=sys.stderr)
            exit(1)
        
        self.__BASE_DOMAIN = "https://api.themoviedb.org/3"
        self.__API_READ_TOKEN: Final = TMDB_READ_ACCESS_TOKEN
        self._session = requests.Session()
        self._session.headers["Authorization"] = f"Bearer {TMDB_READ_ACCESS_TOKEN}"
        
    
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


    # how many top-billed cast members to keep per title
    TOP_CAST = 8
    # movie crew jobs kept as "people" features
    MOVIE_CREW_JOBS = {"Director": "director", "Screenplay": "writer", "Writer": "writer"}

    def fetch_title_details(self, media_type: Literal["movie", "tv"], tmdb_id: int | str, retries: int = 3) -> Optional[dict]:
        """
        Fetches the content metadata of one movie or TV show in a single request (details + keywords + credits).

        Args:
            media_type ("movie" | "tv"): TMDB media type - Emby "Movie" = "movie", "Series" = "tv"
            tmdb_id (int | str): TMDB ID
            retries (int, optional): Retries on rate limiting (HTTP 429) and server errors - DEFAULT: 3

        Returns:
            dict, or None if TMDB has no such title (HTTP 404):
                tmdb_id, media_type, imdb_id, title, overview, original_language, genres (list[str]),
                keywords (list[str]), collection (str | None - movie franchise, e.g. "The Accountant Collection"),
                people (list of {id, name, role}: role is cast/director/writer/creator),
                recommendations (list of "movie:<id>" / "tv:<id>": TMDB's "people who liked this also liked" list,
                from its users' behaviour - first page, up to 20)

        Raises:
            requests.HTTPError: any other failed request
        """
        extra = "keywords,credits,recommendations" if media_type == "movie" else "keywords,aggregate_credits,external_ids,recommendations"
        d = self._get(f"/{media_type}/{tmdb_id}", {"append_to_response": extra, "language": "en-US"}, retries)
        if d is None:
            return None

        if media_type == "movie":
            keywords = d.get("keywords", {}).get("keywords", [])
            credits = d.get("credits", {})
            crew = [
                {"id": c["id"], "name": c["name"], "role": self.MOVIE_CREW_JOBS[c.get("job")]}
                for c in credits.get("crew", []) if c.get("job") in self.MOVIE_CREW_JOBS
            ]
            imdb_id = d.get("imdb_id")
        else:
            keywords = d.get("keywords", {}).get("results", [])
            credits = d.get("aggregate_credits", {})
            crew = [{"id": c["id"], "name": c["name"], "role": "creator"} for c in d.get("created_by", [])]
            imdb_id = d.get("external_ids", {}).get("imdb_id")

        cast = sorted(credits.get("cast", []), key=lambda c: c.get("order", 1_000_000))[:self.TOP_CAST]
        people, seen = [], set()
        for p in crew + [{"id": c["id"], "name": c["name"], "role": "cast"} for c in cast]:
            # the same person can be credited twice, e.g. as both Screenplay and Writer
            if (p["id"], p["role"]) not in seen:
                seen.add((p["id"], p["role"]))
                people.append(p)

        collection = d.get("belongs_to_collection")
        return {
            "tmdb_id": int(d["id"]),
            "media_type": media_type,
            "imdb_id": imdb_id or None,
            "title": d.get("title") or d.get("name"),
            "overview": d.get("overview") or "",
            "original_language": d.get("original_language"),
            "genres": [g["name"] for g in d.get("genres", [])],
            "keywords": [k["name"] for k in keywords],
            "collection": collection.get("name") if collection else None,
            "people": people,
            "recommendations": [
                f"{r.get('media_type') or media_type}:{r['id']}" for r in d.get("recommendations", {}).get("results", [])
            ],
        }

    def find_by_imdb_id(self, imdb_id: str, retries: int = 3) -> Optional[tuple[str, int]]:
        """
        Finds the TMDB movie or show for an IMDB ID. An episode's IMDB ID resolves to its show, which is how shows that
        were removed from the library (so have no TMDB link of their own) are found from their episodes. When TMDB has
        several entries for the ID (e.g. a junk "movie" claiming a series' ID), the one with the most votes wins.

        Returns:
            ("movie" | "tv", tmdb_id), or None if TMDB doesn't know the ID
        """
        d = self._get(f"/find/{imdb_id}", {"external_source": "imdb_id"}, retries)
        if d is None:
            return None
        found = [(r.get("vote_count") or 0, r.get("popularity") or 0, media, int(r["id"]))
                 for media in ("movie", "tv") for r in d.get(f"{media}_results") or []]
        if found:
            _, _, media, tmdb_id = max(found, key=lambda f: f[:2])
            return media, tmdb_id
        if d.get("tv_episode_results"):
            return "tv", int(d["tv_episode_results"][0]["show_id"])
        return None

    def search_title(self, query: str, retries: int = 3) -> Optional[dict]:
        """
        TMDB's best match for a movie or show name (its multi search, ranked by relevance and popularity).

        Returns:
            {"media_type": "movie" | "tv", "tmdb_id": int, "year": int | None}, or None if nothing matches
        """
        d = self._get("/search/multi", {"query": query, "include_adult": "false", "language": "en-US"}, retries)
        for r in (d or {}).get("results", []):
            if r.get("media_type") in ("movie", "tv"):
                year = (r.get("release_date") or r.get("first_air_date") or "")[:4]
                return {"media_type": r["media_type"], "tmdb_id": int(r["id"]), "year": int(year) if year.isdigit() else None}
        return None

    def _get(self, path: str, params: dict, retries: int) -> Optional[dict]:
        """GET a TMDB API path; None on 404, retrying rate limiting (HTTP 429) and server errors"""
        for attempt in range(retries + 1):
            res = self._session.get(f"{self.__BASE_DOMAIN}{path}", params=params, timeout=30)
            if res.status_code == 404:
                return None
            if (res.status_code == 429 or res.status_code >= 500) and attempt < retries:
                time.sleep(float(res.headers.get("Retry-After") or 2 ** attempt))
                continue
            res.raise_for_status()
            return res.json()
