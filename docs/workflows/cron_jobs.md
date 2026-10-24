# Scheduled Jobs

EMBRACE has two scheduled jobs:
- **Refresh:** the daily Emby watch history refresh. It can be scheduled with a **systemd user timer** (used on optiplex01) or with **cron**; both run the same Python script.
- **Picks:** the nightly recommendation playlists, which run straight after the refresh (systemd only).

## systemd timer (optiplex01)

Unit files live in `scripts/systemd/` and assume the repo is at `~/src/EMBRACE` with a venv at `~/src/EMBRACE/venv`. A timer catches up a run missed while the machine was off (`Persistent=true`) and never runs two refreshes at once.

```sh
mkdir -p ~/.config/systemd/user
ln -sf ~/src/EMBRACE/scripts/systemd/embrace-refresh.{service,timer} ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now embrace-refresh.timer
loginctl enable-linger "$USER"     # keep user timers running without a login session

systemctl --user start embrace-refresh.service   # run once now
journalctl --user -u embrace-refresh -n 50       # logs
systemctl --user list-timers embrace-refresh.timer
```

The canonical SQLite database is the one on the machine running the refresh (optiplex01: `~/src/EMBRACE/sqlite_db/`). To work with a current copy elsewhere:

```sh
scp optiplex01:src/EMBRACE/sqlite_db/EMBRACE_db.db sqlite_db/
```

### Daily picks

`embrace-picks.service` runs `scripts/picks/daily_picks.py` (see [Daily picks](../modules/ml.md#daily-picks-recommender-mlpicks)).
- **Trigger:** it has no timer of its own. Enabling it links it to the refresh, which pulls it in on every run.
- **Order:** it starts once the refresh has finished, even if the refresh failed, since yesterday's data still makes fair picks.

```sh
systemctl --user enable ~/src/EMBRACE/scripts/systemd/embrace-picks.service   # links it + hooks it to the refresh
systemctl --user start embrace-picks.service      # run once now
journalctl --user -u embrace-picks -n 50
```

The job needs the IMDB MySQL database (or its genre cache in `data/cache/`). For the collaborative signal, it also needs MovieLens in `data/movielens/`.

## Cron


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
2. Makes a working copy of the database (`<SQLITE_DB_NAME>.building`) and refreshes it: TMDB genres (optional), library metadata, TMDB title details for new or stale titles (optional - keywords, cast/crew, descriptions and TMDB recommendations), Emby user item states (optional - played flags), Jellyseerr requests (optional, when configured), new watch events, then rebuilds sessions and stats
3. Replaces the live database with the working copy **only if every step succeeded** - on failure the working copy is discarded, the live database is untouched, and a failure message is sent to Discord

Watch events are added incrementally rather than rebuilt, because Playback Reporting only keeps ~90 days of history.

The mentioned python script is located under `scripts/sqlite/emby_refresh_watch_hist.py`.
