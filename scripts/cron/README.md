# Cron Job Scripts

## `refresh_emby_watch_hist.sh`

Don't forget to run the following before attempting to execute the script:
```sh
chmod +x refresh_emby_watch_hist.sh
```
**IMPORTANT:** Assumes the python script is located at `{project root}/scripts/python/emby_refresh_user_watch_hist.py`

### What this script does

- Finds project root (assumes script is in {project root}/scripts/cron/)
- Finds python interpreter, prioritises project's venv if available, otherwise system pytthon3, otherwise error
- Builds a cron-safe command:
    - cd into project root
    - run Python script
    - log output
    - use `flock` to avoid overlap
- Updates crontab idempotently: removes any old copy of the job, inserts the new one
- Supports --remove and --run for convenience
