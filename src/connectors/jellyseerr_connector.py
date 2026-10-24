import os
import sys
import time
from typing import Final, Iterable, Optional

import requests


class JellyseerrConnector:
    """
    Reads media requests from Jellyseerr (who asked for which movie/show, and when).

    Needs JELLYSEERR_URL (e.g. https://requests.example.com) and JELLYSEERR_API_KEY (Jellyseerr: Settings -> General ->
    API Key) in the environment, or passed in.
    """

    # https://api-docs.overseerr.dev (Jellyseerr shares Overseerr's API)
    REQUEST_STATUS = {1: "pending", 2: "approved", 3: "declined", 4: "failed", 5: "completed"}
    MEDIA_STATUS = {1: "unknown", 2: "pending", 3: "processing", 4: "partially_available", 5: "available", 6: "deleted"}

    def __init__(self, base_url: Optional[str] = None, api_key: Optional[str] = None, debug: bool = False):
        self._debug: Final = debug
        base_url = base_url or os.getenv("JELLYSEERR_URL")
        api_key = api_key or os.getenv("JELLYSEERR_API_KEY")
        if not base_url or not api_key:
            raise ValueError("JELLYSEERR_URL and JELLYSEERR_API_KEY must be set")
        self._base: Final = base_url.rstrip("/") + "/api/v1"
        self._session = requests.Session()
        self._session.headers["X-Api-Key"] = api_key

    def iter_requests(self, page_size: int = 100, retries: int = 3) -> Iterable[dict]:
        """
        Yields every request (all statuses), newest first.

        Yields:
            dict: request_id, media_type ("movie" | "tv"), tmdb_id, status / media_status (see REQUEST_STATUS /
            MEDIA_STATUS - names, or the raw number if unknown), requested_at, updated_at (ISO strings), requester_id
            (Jellyseerr user ID), requester_name, requester_emby_id (the requester's Emby/Jellyfin user ID, when their
            Jellyseerr account is linked to one)
        """
        skip = 0
        while True:
            page = self._get("/request", {"take": page_size, "skip": skip, "filter": "all", "sort": "added"}, retries)
            results = page.get("results", [])
            for r in results:
                media = r.get("media") or {}
                user = r.get("requestedBy") or {}
                if media.get("tmdbId") is None:
                    continue
                yield {
                    "request_id": int(r["id"]),
                    "media_type": r.get("type") or media.get("mediaType"),
                    "tmdb_id": int(media["tmdbId"]),
                    "status": self.REQUEST_STATUS.get(r.get("status"), r.get("status")),
                    "media_status": self.MEDIA_STATUS.get(media.get("status"), media.get("status")),
                    "requested_at": r.get("createdAt"),
                    "updated_at": r.get("updatedAt"),
                    "requester_id": user.get("id"),
                    "requester_name": user.get("displayName") or user.get("jellyfinUsername") or user.get("username"),
                    "requester_emby_id": user.get("jellyfinUserId"),
                }
            skip += len(results)
            total = (page.get("pageInfo") or {}).get("results", 0)
            if not results or skip >= total:
                break
            if self._debug: print(f"[Jellyseerr Connector] requests: {skip}/{total}")

    def _get(self, path: str, params: dict, retries: int) -> dict:
        for attempt in range(retries + 1):
            res = self._session.get(f"{self._base}{path}", params=params, timeout=30)
            if (res.status_code == 429 or res.status_code >= 500) and attempt < retries:
                time.sleep(float(res.headers.get("Retry-After") or 2 ** attempt))
                continue
            if res.status_code in (401, 403):
                print("[Jellyseerr Connector] ERROR: API key rejected", file=sys.stderr)
            res.raise_for_status()
            return res.json()
        raise RuntimeError("unreachable")
