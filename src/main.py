import sys
import requests
from dotenv import load_dotenv
import os
from typing import Final

load_dotenv()   # required to load .env

BASE_DOMAIN: Final = os.getenv("BASE_DOMAIN")
EMBY_API_KEY: Final = os.getenv("EMBY_API_KEY")

# exit if variables are unavailable
if BASE_DOMAIN is None:
    print("Error: BASE_DOMAIN environment variable is not set.", file=sys.stderr)
    exit(1)
if EMBY_API_KEY is None:
    print("Error: EMBY_API_KEY environment variable is not set.", file=sys.stderr)
    exit(1)

# check emby server can be reached
get_base = requests.get(f"{BASE_DOMAIN}/System/Ping?api_key={EMBY_API_KEY}")
if not get_base.ok:
    print(f"Error: Emby server at {BASE_DOMAIN} is unavailable. Status code: {get_base.status_code}", file=sys.stderr)
    exit(1)
else:
    print(f"Emby server available at [{BASE_DOMAIN}]! Status: {get_base.status_code}")


# --- user functions ---
def get_all_emby_users() -> dict[str, str]:
    """
    Fetch all Emby users (including hidden users)

    Returns:
        dict: keys = usernames, values = Emby user ID
    """
    response = requests.get(f"{BASE_DOMAIN}/Users/Query?IsHidden=true&api_key={EMBY_API_KEY}")
    response.raise_for_status()
    get_users = response.json()
    users: dict[str, str] = {}
    for user in get_users["Items"]:
        users[user["Name"]] = user["Id"]
    return users


def get_user_watch_hist(user_id: str, num_days: int, is_aggregated=False,):
    
    """
    Fetch the watch history for a user
    Args:
        user_id (str): User's Emby ID
        num_days (int): The number of days to look back for watch history
        is_aggregated (bool, optional): Whether to aggregate the data, defaults to False
    Returns:
        dict: The user's watch history data as a JSON-decoded dictionary
    """
    return requests.get(f"{BASE_DOMAIN}/user_usage_stats/UserPlaylist?user_id={user_id}&aggregate_data={is_aggregated}&days={num_days}&api_key={EMBY_API_KEY}'").json()

