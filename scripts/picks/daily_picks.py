"""
Daily picks: each user's recommendations as two private Emby playlists ("Fresh for you", "From the back catalogue"),
plus a preview of everyone's picks to the Discord webhook. Each run is appended to the `recommendation_log` table.
Runs after the nightly refresh (scripts/systemd/embrace-picks.service); see docs/workflows/cron_jobs.md.

usage (from project root): PYTHONPATH=src python scripts/picks/daily_picks.py [--dry-run] [--no-emby] [--no-discord]
                                                                             [--users NAME,...] [-n 10]
"""
import argparse
import os
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
from dotenv import load_dotenv

from connectors import EmbyConnector
from ml import PreProcess, Recommender
from ml.picks import BACK_CATALOGUE, FRESH
from utils import Notifications

ROOT_DIR = Path(__file__).resolve().parents[2]
os.chdir(ROOT_DIR)  # SQLiteConnector resolves "sqlite_db/" relative to the working directory
load_dotenv(ROOT_DIR / ".env")

PLAYLIST_NAMES = {FRESH: "Fresh for you", BACK_CATALOGUE: "From the back catalogue"}
LIST_LABELS = {FRESH: "new", BACK_CATALOGUE: "back catalogue"}
# picks per list shown in the Discord preview (the playlists get all of them)
PREVIEW_PER_LIST = 3
tz = ZoneInfo("Australia/Melbourne")


def parse_args():
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--dry-run", action="store_true", help="print the picks only: no playlists, Discord or log")
    p.add_argument("--no-emby", action="store_true", help="don't update playlists")
    p.add_argument("--no-discord", action="store_true", help="print the preview instead of sending it")
    p.add_argument("--users", help="comma-separated Emby user names (default: everyone eligible)")
    p.add_argument("-n", type=int, default=10, help="titles per list (default 10)")
    return p.parse_args()


def fail(step: str, e: Exception, notify: bool):
    msg = f"Daily picks FAILED at step: {step} ({type(e).__name__}: {e})\nPlaylists were left as they were."
    print(f"[ERROR] {msg}", file=sys.stderr)
    if notify:
        Notifications().discord_send_webhook(msg)
    exit(1)


def label(row) -> str:
    year = f" ({row.start_year})" if pd.notna(row.start_year) and str(row.start_year).strip() else ""
    requested = " (requested)" if pd.notna(row.you_requested) and bool(row.you_requested) else ""
    return f"{row.primary_name}{year}{requested}"


def preview(picks: pd.DataFrame, statuses: dict[str, str], when: datetime) -> list[str]:
    lines = [f"**Daily picks** - {when.strftime('%a %d %b')}"]
    for user_name, mine in picks.groupby("user_name", sort=True):
        lines.append(f"**{user_name}**" + (f" - playlists {statuses[user_name]}" if user_name in statuses else ""))
        for lst in (FRESH, BACK_CATALOGUE):
            top = mine[mine["list"] == lst].nsmallest(PREVIEW_PER_LIST, "rank")
            if len(top):
                lines.append(f"  {LIST_LABELS[lst]}: " + ", ".join(label(r) for r in top.itertuples()))
    return lines


def main():
    args = parse_args()
    notify = not (args.dry_run or args.no_discord)
    started = datetime.now(tz=tz)
    try:
        emby = EmbyConnector()
        emby_users = emby.get_all_emby_users()      # name -> current ID: picks only go to people still on the server
        excluded = {u.strip().casefold() for u in (os.getenv("EMBRACE_PICKS_EXCLUDE_USERS") or "").split(",") if u.strip()}
        wanted = {u.strip().casefold() for u in args.users.split(",")} if args.users else None
        names = {uid: name for name, uid in emby_users.items()
                 if name.casefold() not in excluded and (wanted is None or name.casefold() in wanted)}
    except Exception as e:
        fail("Emby users", e, notify)

    try:
        pp = PreProcess()
        rec = Recommender.load(pp)
        picks = rec.all_picks(n=args.n, users=set(names))
        picks["user_name"] = picks["user_id"].map(names)
        picks["item_id"] = picks["t_const"].map(pp.get_playlist_items())
    except Exception as e:
        fail("recommendations", e, notify)
    model = type(rec.knn).__name__
    print(f"[INFO] {model}: {picks['user_id'].nunique()} users, {len(picks)} picks")

    if args.dry_run:
        cols = ["user_name", "list", "rank", "primary_name", "start_year", "score", "because_you_watched",
                "added_days_ago", "you_requested", "item_id"]
        with pd.option_context("display.width", 200, "display.max_rows", None, "display.max_colwidth", 40):
            print(picks[cols].to_string(index=False))
        print("\n".join(preview(picks, {}, started)))
        return

    # ---- playlists: one user failing doesn't stop the others ----
    statuses: dict[str, str] = {}
    failures = 0
    if not args.no_emby:
        for user_id, mine in picks.groupby("user_id"):
            results = []
            try:
                for lst, name in PLAYLIST_NAMES.items():
                    items = mine[mine["list"] == lst].sort_values("rank")["item_id"].dropna().tolist()
                    results.append(emby.set_user_playlist(user_id, name, items))
            except Exception as e:
                print(f"[ERROR] playlists for {names[user_id]}: {type(e).__name__}: {e}", file=sys.stderr)
                results.append("FAILED")
                failures += 1
            statuses[names[user_id]] = "/".join(dict.fromkeys(results))

    logged = pp.sqlite.log_recommendations(picks, model=model, generated_at=started.strftime("%Y-%m-%d %H:%M:%S"))
    if logged < 0:
        print("[WARN] recommendations were not logged", file=sys.stderr)

    lines = preview(picks, statuses, started)
    if failures:
        lines.append(f"Playlist updates FAILED for {failures} user(s), see the journal")
    if notify:
        Notifications().discord_send_lines(lines)
    else:
        print("\n".join(lines))
    exit(1 if failures else 0)


if __name__ == "__main__":
    main()
