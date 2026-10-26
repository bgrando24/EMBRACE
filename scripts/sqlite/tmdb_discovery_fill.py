import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

from connectors import SQLiteConnector, TMDBConnector
from ml import PreProcess

# fetches TMDB details for the titles beyond the library now (the chat-bot's recommendations beyond the server), rather
# than over several nightly refreshes, which fetch at most DISCOVERY_PER_RUN each. A first fill takes about 1.5 hours.
# Writes the live database: not while the nightly refresh runs, since its working copy would replace these writes.
#
# usage (from project root):
#   PYTHONPATH=src python scripts/sqlite/tmdb_discovery_fill.py [--limit N]

load_dotenv()
os.chdir(Path(__file__).resolve().parents[2])  # SQLiteConnector resolves "sqlite_db/" relative to the working directory

parser = argparse.ArgumentParser(description="Fetch TMDB details for titles beyond the library, for the chat-bot")
parser.add_argument("--limit", type=int, help="at most this many lookups and fetches (DEFAULT: everything due)")
args = parser.parse_args()

SQLITE_DB_NAME = os.getenv("SQLITE_DB_NAME") or "EMBRACE_SQLITE_DB.db"
if Path("sqlite_db", f"{SQLITE_DB_NAME}.building").exists():
    print("[ERROR] The nightly refresh is running (its working copy exists): try again when it's finished", file=sys.stderr)
    exit(1)

pp = PreProcess(SQLITE_DB_NAME)
popular = pp.get_popular_imdb_ids(SQLiteConnector.DISCOVERY_MIN_VOTES)
print(f"[INFO] {len(popular)} IMDB titles with {SQLiteConnector.DISCOVERY_MIN_VOTES}+ votes")
TMDB = TMDBConnector(os.getenv("TMDB_READ_ACCESS_TOKEN"))
fetched = pp.sqlite.ingest_tmdb_discovery_details(TMDB.fetch_title_details, TMDB.find_by_imdb_id, popular, limit=args.limit)
d = pp.sqlite.get_tmdb_title_details()
print(f"[INFO] fetched {fetched}; titles with details: {int(d['in_library'].sum())} library, {int((~d['in_library']).sum())} beyond it")
exit(1 if fetched < 0 else 0)
