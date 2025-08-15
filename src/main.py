# pip modules
import sys
import requests
from dotenv import load_dotenv
import os
from typing import Final

# our modules
from emby_connector import EmbyConnector
from sqlite_connector import SQLiteConnector

# load and extract env variables
load_dotenv()
BASE_DOMAIN: Final = os.getenv("BASE_DOMAIN")
EMBY_API_KEY: Final = os.getenv("EMBY_API_KEY")
ENVIRONMENT: Final = os.getenv("ENVIRONMENT") or "dev"
SQLITE_DB_NAME: Final = os.getenv("SQLITE_DB_NAME") or "EMBRACE_SQLITE_DB.db"

# init emby API conenctor
Emby = EmbyConnector(BASE_DOMAIN, EMBY_API_KEY, debug=(ENVIRONMENT == "dev"))

# testing functions
users = Emby.get_all_emby_users()
# print(users)
bgmd_hist = Emby.get_user_watch_hist(users["bgmd"], 10)
# print(bgmd_hist)

# test db connection
sqlite = SQLiteConnector(SQLITE_DB_NAME, debug=True)
sqlite.connect_db()
result = sqlite.INIT_create_user_watch_hist_schemas()
print(f"DB INIT_create_user_watch_hist_schemas result: {result}")