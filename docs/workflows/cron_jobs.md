# Cron Jobs

EMBRACE currently only has one cron job implemented, but more in the future are planned.


### Emby Watch History Refresh

**TODO:** add environment variable for custom discord webhook

Located under: `scripts/cron/emby_refresh_watch_hist.sh`

The purpose of this cron job is to refresh the Emby watch history database daily at 00:00.

This script has three modes, you can also use the "--help" flag to see these options:
- `install_refresh_cron.sh`          - install or update the cron job
- `install_refresh_cron.sh --remove` - remove the cron job
- `install_refresh_cron.sh --run`    - run the job once now (same command cron would run)

Running this script will add a cron job to the executing machine which does the following:
1. Copies the current SQLite database to sqlite\_db/backups, with the current date + time (Australia/Melbourne timezone) and ".backup" appended
2. Makes a working copy of the database (`<SQLITE_DB_NAME>.building`) and refreshes it: TMDB genres (optional), library metadata, new watch events, then rebuilds sessions and stats
3. Replaces the live database with the working copy **only if every step succeeded** - on failure the working copy is discarded, the live database is untouched, and a failure message is sent to Discord

Watch events are added incrementally rather than rebuilt, because Playback Reporting only keeps ~90 days of history.

The mentioned python script is located under `scripts/sqlite/emby_refresh_watch_hist.py`.
