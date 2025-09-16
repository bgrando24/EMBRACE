from dotenv import load_dotenv
import os
from typing import Final

from emby_connector import EmbyConnector
from sqlite_connector import SQLiteConnector
from tmdb_connector import TMDBConnector
from analytics import Analytics

# load and extract env variables
load_dotenv()
BASE_DOMAIN: Final = os.getenv("BASE_DOMAIN")
EMBY_API_KEY: Final = os.getenv("EMBY_API_KEY")
TMDB_READ_ACCESS_TOKEN: Final = os.getenv("TMDB_READ_ACCESS_TOKEN")
ENVIRONMENT: Final = os.getenv("ENVIRONMENT") or "dev"
SQLITE_DB_NAME: Final = os.getenv("SQLITE_DB_NAME") or "EMBRACE_SQLITE_DB.db"

# init API connectors
Emby = EmbyConnector(BASE_DOMAIN, EMBY_API_KEY, debug=(ENVIRONMENT == "dev"))
TMDB = TMDBConnector(TMDB_READ_ACCESS_TOKEN, debug=(ENVIRONMENT == "dev"))

# testing functions
# users = Emby.get_all_emby_users()
# print(users)
# bgmd_hist = Emby.get_user_watch_hist(users["bgmd"], 10)
# print(bgmd_hist)
# all_user_hist = Emby.get_all_watch_hist(1)
# print(all_user_hist)

# test db connection and watch history tables
sqlite = SQLiteConnector(SQLITE_DB_NAME, debug=True)
# try: 
#     os.remove("sqlite_db/bgmd_db.db")
# except:
#     pass

sqlite.connect_db()

# ingest library metadata first so runtime is available for later calculations
# sqlite._INIT_create_library_items_schema()
# ok = sqlite.ingest_all_library_items(Emby.iter_all_items(), Emby.get_item_metadata)
# print("Ingest complete:", ok)

# # process watch history using actual runtimes
# sqlite._INIT_POPULATE_watch_hist_raw_events(Emby.get_all_watch_hist)
# sqlite._INIT_POPULATE_watch_hist_agg_sessions()
# sqlite._INIT_POPULATE_watch_hist_user_item_stats()
# sqlite.update_completion_ratios()

# #create and ingest TMDB tables
# sqlite._INIT_create_tmdb_schemas()
# sqlite.ingest_tmdb_movie_tv_genres(TMDB.fetch_movie_genres, TMDB.fetch_tv_genres)

# basic analytics testing
if sqlite._connection is not None:
    A = Analytics(sqlite._connection)
else:
    raise RuntimeError("Failed to establish SQLite connection for Analytics class")

print(A.get_user_item_stats())