#!/bin/sh

# This script adds/removes a cron job to the executing machine, that runs a DAILY fetch of the Emby
# 	user watch history and stores it into the local SQLite database.
# Under the hood, the cron job just runs the "{SCRIPT NAME}.py python script 


# e = exit immediately if any cmd fails, u = unset vars error, o pipefail = fail pipeline if any cmd fail
set -euo pipefail



#==================
# Config variables
#==================

# CRON_SCHEDULE: when the job should run - for syntax reference: https://docs.gitlab.com/topics/cron/#cron-syntax
"${CRON_SCHEDULE:=@daily}"
# cron by default uses system timezone, but setting this just to be safe
"${CRON_TZ:=Australia/Melbourne}"
# CRON_TAG: label that wraps the cron entry between "BEGIN/END" blocks - helpful for removing job later
"${CRON_TAG:=emby_watch_hist_refresh}"



#===================
# Directory pathing
#===================

# folder 'this' script lives in
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
# project's root: assumes this file lives in scripts/cron/, or two levels down from project root
PROJECT_ROOT="$(realpath "${SCRIPT_DIR}/../..")"
# path of python script to run, relative to project root
PY_SCRIPT="${PROJECT_ROOT}/scripts/sqlite/refresh_emby_watch_hist.py"




#====================
# Python interpreter
#====================
# interpreter preference: project's venv -> system python3 -> otherwise fail
# TODO: also have fallback for just 'python'?
