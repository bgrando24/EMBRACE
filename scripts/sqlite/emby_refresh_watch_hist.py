from connectors import SQLiteConnector, EmbyConnector, TMDBConnector
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path
from dotenv import load_dotenv
from utils import Notifications
import os
import shutil

# this script rebuilds the emby user watch history database, and saves the old database as a backup

load_dotenv()

# =======================
# Backup current DB file
# =======================

# get 'this' script's absolute path
SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parents[1]
SQLITE_DIR = (ROOT_DIR / "sqlite_db").resolve()
BACKUP_DIR = (SQLITE_DIR / "backups").resolve()

# create backup
tz = ZoneInfo("Australia/Melbourne")
today_date = datetime.now(tz=tz).strftime("%Y%m%d-%H:%M:%S")

SQLITE_DIR.mkdir(parents=True, exist_ok=True)
BACKUP_DIR.mkdir(parents=True, exist_ok=True)
SQLITE_DB_NAME = os.getenv("SQLITE_DB_NAME") or "EMBRACE_SQLITE_DB.db"

db_path = (SQLITE_DIR / SQLITE_DB_NAME).resolve()
legacy_db_path = (ROOT_DIR / "sqlite_db" / SQLITE_DB_NAME).resolve()
if not db_path.exists() and legacy_db_path.exists():
    print(f"[INFO] SQLite database found in legacy path: {legacy_db_path}")
    db_path = legacy_db_path

backup_path = BACKUP_DIR / f"{SQLITE_DB_NAME}_{today_date}.backup"

if not db_path.exists():
    print(f"[WARN] SQLite database not found: {db_path}")
    print("[INFO] Proceeding without backup - a new database will be created")
else:
    try:
        # copy first, then unlink original after success
        shutil.copy2(db_path, backup_path)
        print(f"[OK] Backup created at {backup_path}")
        db_path.unlink()
    except Exception as e:
        print(f"[ERROR] Failed to back up database: {e}")
        exit(1)
        
        
# =======================
# Create new db file
# =======================
ENVIRONMENT = os.getenv("ENVIRONMENT") or "dev"
TMDB_READ_ACCESS_TOKEN = os.getenv("TMDB_READ_ACCESS_TOKEN")
sqlite = SQLiteConnector(SQLITE_DB_NAME, debug=True)
Emby = EmbyConnector(debug=(ENVIRONMENT == "dev"))
TMDB = TMDBConnector(TMDB_READ_ACCESS_TOKEN, debug=(ENVIRONMENT == "dev"))

sqlite.connect_db()

# ingest library metadata first so runtime is available for later calculations
sqlite._INIT_create_library_items_schema()
ok = sqlite.ingest_all_library_items(Emby.iter_all_items(), Emby.get_item_metadata)
print("Ingest complete:", ok)

# process watch history using actual runtimes
sqlite._INIT_POPULATE_watch_hist_raw_events(Emby.get_all_watch_hist)
sqlite._INIT_POPULATE_watch_hist_agg_sessions()
sqlite._INIT_POPULATE_watch_hist_user_item_stats()
sqlite.update_completion_ratios()

 # create and ingest TMDB tables
sqlite._INIT_create_tmdb_schemas()
sqlite.ingest_tmdb_movie_tv_genres(TMDB.fetch_movie_genres, TMDB.fetch_tv_genres)

finished_at = datetime.now(tz=tz).strftime("%Y-%m-%d %H:%M:%S %Z")
Notifications().discord_send_webhook(
    f"Cron job: Emby watch history refresh finished at {finished_at}\n"
    f"Ingest complete: {ok}\n"
    f"Backup: {backup_path.name}"
    )

exit(0)