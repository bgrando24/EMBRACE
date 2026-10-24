from connectors import SQLiteConnector, EmbyConnector, TMDBConnector
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path
from dotenv import load_dotenv
from utils import Notifications
import os
import shutil
import sys

# this script refreshes the Emby library + watch history database, keeping a dated backup of the previous version
#
# NOTE: Playback Reporting only keeps a limited window of history (~90 days as of Oct 2026), so watch events are
# ADDED to the existing database rather than rebuilt from scratch - older history only exists in our own DB.
# The refresh runs against a working copy, which only replaces the live database once every step succeeds.

load_dotenv()

# get 'this' script's absolute path
SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parents[1]
SQLITE_DIR = (ROOT_DIR / "sqlite_db").resolve()
BACKUP_DIR = (SQLITE_DIR / "backups").resolve()

tz = ZoneInfo("Australia/Melbourne")
today_date = datetime.now(tz=tz).strftime("%Y%m%d-%H:%M:%S")

SQLITE_DIR.mkdir(parents=True, exist_ok=True)
BACKUP_DIR.mkdir(parents=True, exist_ok=True)
SQLITE_DB_NAME = os.getenv("SQLITE_DB_NAME") or "EMBRACE_SQLITE_DB.db"
WORKING_DB_NAME = f"{SQLITE_DB_NAME}.building"

db_path = SQLITE_DIR / SQLITE_DB_NAME
working_path = SQLITE_DIR / WORKING_DB_NAME
backup_path = BACKUP_DIR / f"{SQLITE_DB_NAME}_{today_date}.backup"


def fail(step: str):
    """Discard the working copy, leave the live database untouched, notify and exit"""
    working_path.unlink(missing_ok=True)
    msg = f"Cron job: Emby watch history refresh FAILED at step: {step}\nLive database was left unchanged."
    print(f"[ERROR] {msg}", file=sys.stderr)
    Notifications().discord_send_webhook(msg)
    exit(1)


# =======================
# Backup + working copy
# =======================
working_path.unlink(missing_ok=True)    # leftover from a previous failed run

if not db_path.exists():
    print(f"[WARN] SQLite database not found: {db_path}")
    print("[INFO] Proceeding without backup - a new database will be created")
else:
    try:
        shutil.copy2(db_path, backup_path)
        print(f"[OK] Backup created at {backup_path}")
        shutil.copy2(db_path, working_path)
    except Exception as e:
        print(f"[ERROR] Failed to back up database: {e}", file=sys.stderr)
        exit(1)


# =======================
# Refresh working copy
# =======================
ENVIRONMENT = os.getenv("ENVIRONMENT") or "dev"
TMDB_READ_ACCESS_TOKEN = os.getenv("TMDB_READ_ACCESS_TOKEN")
os.chdir(ROOT_DIR)  # SQLiteConnector resolves "sqlite_db/" relative to the working directory
sqlite = SQLiteConnector(WORKING_DB_NAME, debug=True)
Emby = EmbyConnector(debug=(ENVIRONMENT == "dev"))
TMDB = TMDBConnector(TMDB_READ_ACCESS_TOKEN, debug=(ENVIRONMENT == "dev"))

sqlite.connect_db()

try:
    # TMDB genres first, so the library ingest can map Emby genre names to TMDB IDs
    # optional: without them, genre IDs fall back to stable name hashes
    sqlite._INIT_create_tmdb_schemas()
    try:
        if sqlite.ingest_tmdb_movie_tv_genres(TMDB.fetch_movie_genres, TMDB.fetch_tv_genres) is False:
            print("[WARN] TMDB genre ingest failed, continuing with existing/hashed genre IDs", file=sys.stderr)
    except Exception as e:
        print(f"[WARN] TMDB genre ingest failed ({e}), continuing with existing/hashed genre IDs", file=sys.stderr)

    # library metadata before watch history, so runtime is available for later calculations
    if not sqlite.ingest_all_library_items(Emby.iter_all_items(), Emby.get_item_metadata):
        fail("library ingest")

    new_events = sqlite.ingest_watch_hist_raw_events(Emby.get_all_watch_hist)
    if new_events < 0:
        fail("watch history ingest")

    # sessions + stats are derived entirely from the raw events, so they're rebuilt each run
    if not sqlite._INIT_POPULATE_watch_hist_agg_sessions():
        fail("session aggregation")
    if not sqlite._INIT_POPULATE_watch_hist_user_item_stats():
        fail("user-item stats")
    sqlite.update_completion_ratios()
except Exception as e:
    print(f"[ERROR] {type(e).__name__}: {e}", file=sys.stderr)
    fail(f"unexpected error ({type(e).__name__})")

sqlite._connection.close()
os.replace(working_path, db_path)

# keep the newest backups only (one is made per run); the archive/ dir is never touched
KEEP_BACKUPS = int(os.getenv("SQLITE_KEEP_BACKUPS") or 30)
backups = sorted(BACKUP_DIR.glob(f"{SQLITE_DB_NAME}_*.backup"), key=lambda p: p.stat().st_mtime, reverse=True)
for old in backups[KEEP_BACKUPS:]:
    old.unlink()
    print(f"[INFO] Removed old backup {old.name}")

finished_at = datetime.now(tz=tz).strftime("%Y-%m-%d %H:%M:%S %Z")
Notifications().discord_send_webhook(
    f"Cron job: Emby watch history refresh finished at {finished_at}\n"
    f"New watch events: {new_events}\n"
    f"Backup: {backup_path.name if backup_path.exists() else 'none (new database)'}"
    )

exit(0)
