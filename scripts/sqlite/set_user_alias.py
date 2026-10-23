import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

from connectors import SQLiteConnector

# records that two Emby user IDs are the same person (e.g. after an Emby server rebuild), see SQLiteConnector.set_user_alias
#
# usage (from project root):
#   PYTHONPATH=src python scripts/sqlite/set_user_alias.py --list
#   PYTHONPATH=src python scripts/sqlite/set_user_alias.py <old user id> <canonical user id> [--note "..."]

load_dotenv()
os.chdir(Path(__file__).resolve().parents[2])  # SQLiteConnector resolves "sqlite_db/" relative to the working directory

parser = argparse.ArgumentParser(description="Map an old/other Emby user ID to the canonical user ID of the same person")
parser.add_argument("alias_user_id", nargs="?")
parser.add_argument("user_id", nargs="?", help="canonical user ID")
parser.add_argument("--note")
parser.add_argument("--list", action="store_true", help="show watch history user IDs and current aliases")
args = parser.parse_args()

sqlite = SQLiteConnector(os.getenv("SQLITE_DB_NAME") or "EMBRACE_SQLITE_DB.db")
sqlite.connect_db()

if args.list:
    users = sqlite._connection.execute(
        """SELECT user_id, user_name, COUNT(*), MIN(date), MAX(date) FROM watch_hist_raw_events
           GROUP BY user_id ORDER BY user_name, MIN(date)"""
    ).fetchall()
    print(f"{'user_id':34} {'name':12} {'events':>6}  first       last")
    for user_id, name, n, first, last in users:
        print(f"{user_id:34} {name:12} {n:>6}  {first}  {last}")
    print("\nAliases:")
    print(sqlite.get_user_aliases().to_string(index=False))
    exit(0)

if not (args.alias_user_id and args.user_id):
    parser.error("alias_user_id and user_id are required unless --list is given")

known = {row[0] for row in sqlite._connection.execute("SELECT DISTINCT user_id FROM watch_hist_raw_events")}
for uid in (args.alias_user_id, args.user_id):
    if uid not in known:
        print(f"[WARN] {uid} doesn't appear in the watch history", file=sys.stderr)

exit(0 if sqlite.set_user_alias(args.alias_user_id, args.user_id, args.note) else 1)
