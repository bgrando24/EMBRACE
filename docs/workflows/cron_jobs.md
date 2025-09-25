# Cron Jobs

EMBRACE currently only has one cron job implemented, but more in the future are planned.


### Emby Watch History Refresh

**TODO:** add environment variable for custom discord webhook

Located under: `scripts/cron/emby_refresh_watch_hist.sh`

The purpose of this cron job is to refresh the Emby watch history database daily at 00:00.

Running this script will add a cron job to the executing machine which does the following:
1. Renames the current SQLite database file to append the current date + time (Australia/melbourne timezone) and ".backup"
2. Moves the now 'backup' file to sqlite\_db/backups
3. Run the a python script to create a new SQLite database file, build the schema, and import the required Emby data

The mentioned python script is located under `scripts/sqlite/emby_refresh_watch_hist.py`
