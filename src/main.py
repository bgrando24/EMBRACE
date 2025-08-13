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
get_base = requests.get(BASE_DOMAIN)
if not get_base.ok:
    print(f"Error: Emby server at {BASE_DOMAIN} is unavailable. Status code: {get_base.status_code}", file=sys.stderr)
    exit(1)
else:
    print(f"Emby server available at [{BASE_DOMAIN}]! Status: {get_base.status_code}")

