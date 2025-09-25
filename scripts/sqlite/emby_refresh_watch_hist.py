from connectors import SQLiteConnector
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path
from dotenv import load_dotenv
import os
import shutil

# this script rebuilds the emby user watch history database, and saves the old database as a backup

load_dotenv()

# get 'this' script's absolute path
SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parents[1]
SQLITE_DIR = (ROOT_DIR / "sqlite_db").resolve()
BACKUP_DIR = (SQLITE_DIR / "backups").resolve()

# create backup
tz = ZoneInfo("Australia/Melbourne")
today_date = datetime.now(tz=tz).strftime("%Y-%m-%d")

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