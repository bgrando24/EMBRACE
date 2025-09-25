#!/usr/bin/env bash

# This script adds/removes a cron job to the executing machine, that runs a DAILY fetch of the Emby
# 	user watch history and stores it into the local SQLite database.
# Under the hood, the cron job just runs the "{SCRIPT NAME}.py python script 


# e = exit immediately if any cmd fails, u = unset vars error, o pipefail = fail pipeline if any cmd fail
set -euo pipefail



#==================
# Config variables
#==================

# CRON_SCHEDULE: when the job should run - for syntax reference: https://docs.gitlab.com/topics/cron/#cron-syntax
: "${CRON_SCHEDULE:=@daily}"
# cron by default uses system timezone, but setting this just to be safe
: "${CRON_TZ:=Australia/Melbourne}"
# CRON_TAG: label that wraps the cron entry between "BEGIN/END" blocks - helpful for removing job later
: "${CRON_TAG:=emby_watch_hist_refresh}"



#===================
# Directory pathing
#===================
# folder 'this' script lives in
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
# project's root: assumes this file lives in scripts/cron/, or two levels down from project root
PROJECT_ROOT="$(realpath "${SCRIPT_DIR}/../..")"
# path of python script to run, relative to project root
PY_SCRIPT="${PROJECT_ROOT}/scripts/sqlite/emby_refresh_watch_hist.py"




#====================
# Python interpreter
#====================
# interpreter preference: project's venv -> system python3 -> otherwise fail
# TODO: also have fallback for just 'python'?

if [[ -x "${PROJECT_ROOT}/venv/bin/python" ]]; then 
	PYTHON="${PROJECT_ROOT}/venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
	PYTHON="$(command -v python3)"
else
	echo "ERROR: python3 not found and no venv at ${PROJECT_ROOT}/venv/bin/python" >&2
	exit 1
fi




#====================
# Logs & lock file
#====================

# logs dir and files
LOG_DIR="${PROJECT_ROOT}/logs"
TMP_DIR="${PROJECT_ROOT}/tmp"
mkdir -p "${LOG_DIR}"
mkdir -p "${TMP_DIR}"
OUT_LOG="${LOG_DIR}/cron_emby_refresh_watch_hist.out.log"
ERR_LOG="${LOG_DIR}/cron_emby_refresh_watch_hist.err.log"

# lock file: ensures only one instance runs at a time
LOCK_FILE="${TMP_DIR}/${CRON_TAG}.lock"

# common locations for flock
FLOCK_BIN=""
for c in /usr/bin/flock /bin/flock; do
  if [[ -x "$c" ]]; then FLOCK_BIN="$c"; break; fi
done
if [[ -z "$FLOCK_BIN" ]]; then
  echo "WARNING: 'flock' not found. Overlap protection will be disabled." >&2
fi

# =========================
# Helpers
# =========================
print_usage() {
  cat <<'USAGE'
Usage:
  install_refresh_cron.sh            # install or update the cron job
  install_refresh_cron.sh --remove   # remove the cron job
  install_refresh_cron.sh --run      # run the job once now (same command cron would run)

Env overrides:
  CRON_SCHEDULE="15 3 * * *"         # standard cron expression
  CRON_TZ="Australia/Melbourne"      # optional; else system TZ is used
  CRON_TAG="refresh_data_job"        # identifies the cron block
USAGE
}

remove_cron_block() {
  crontab -l 2>/dev/null | awk -v tag="$CRON_TAG" '
    BEGIN {skipping=0}
    $0 ~ "### BEGIN " tag {skipping=1; next}
    $0 ~ "### END " tag {skipping=0; next}
    skipping==0 {print}
  ' | crontab -
}

# Command that cron will run: cd into project, run python, log output
build_job_command() {
  local cmd core="cd \"$PROJECT_ROOT\" && \"$PYTHON\" \"$PY_SCRIPT\""
  if [[ -n "$FLOCK_BIN" ]]; then
    cmd="$FLOCK_BIN -n \"$LOCK_FILE\" /bin/bash -lc '$core >> \"$OUT_LOG\" 2>> \"$ERR_LOG\"'"
  else
    cmd="/bin/bash -lc '$core >> \"$OUT_LOG\" 2>> \"$ERR_LOG\"'"
  fi
  echo "$cmd"
}

install_cron_block() {
  local cmd
  cmd="$(build_job_command)"

  # Dump current crontab (if any) and strip old block
  local current
  current="$(crontab -l 2>/dev/null || true)"
  current="$(awk -v tag="$CRON_TAG" '
    BEGIN {skipping=0}
    $0 ~ "### BEGIN " tag {skipping=1; next}
    $0 ~ "### END " tag {skipping=0; next}
    { if (skipping==0) print }
  ' <<< "$current")"

  {
    # Keep existing lines
    printf "%s\n" "$current" | sed '/^[[:space:]]*$/d'
    echo ""
    echo "### BEGIN ${CRON_TAG} (managed by install_refresh_cron.sh) ###"
    # Optional CRON_TZ line
    if [[ -n "$CRON_TZ" ]]; then
      echo "CRON_TZ=${CRON_TZ}"
    fi
    # Cron entry
    echo "${CRON_SCHEDULE} ${cmd}"
    echo "### END ${CRON_TAG} ###"
  } | crontab -

  echo "Installed/updated cron job '${CRON_TAG}'."
  echo "Schedule: ${CRON_TZ:+(${CRON_TZ}) }${CRON_SCHEDULE}"
  echo "Project : ${PROJECT_ROOT}"
  echo "Python  : ${PYTHON}"
  echo "Logs    : ${OUT_LOG} (stdout), ${ERR_LOG} (stderr)"

  # webhook to notify of successful run
  curl -i -H "Accept: application/json" -H "Content-Type:application/json" -X POST \
	  --data "{\"content\": \"Installed/updated cron job '${CRON_TAG}'\nSchedule: #{CRON_TZ:+(${CRON_TZ}) }${CRON_SCHEDULE}\nProject: ${PROJECT_ROOT}\"}" \
	  https://discord.com/api/webhooks/1420906039242522684/30EaHuuVIb6GiKtIBeTOecSK24JqQpiw-S_iteSL6VQWM1Ma4meNP-hit14hnvfojL4F
}

run_once_now() {
  echo "Running once now with same command cron will use..."
  # shellcheck disable=SC2046
  bash -lc "$(build_job_command | sed -E 's/^[0-9\*].* //')"  # strip cron timing if any
  echo "Done."

  curl -i -H "Accept: application/json" -H "Content-Type:application/json" -X POST \
	  --data "{\"content\": \"Installed/updated cron job '${CRON_TAG}'\nSchedule: #{CRON_TZ:+(${CRON_TZ}) }${CRON_SCHEDULE}\nProject: ${PROJECT_ROOT}\"}" \
	  https://discord.com/api/webhooks/1420906039242522684/30EaHuuVIb6GiKtIBeTOecSK24JqQpiw-S_iteSL6VQWM1Ma4meNP-hit14hnvfojL4F
}

run_once_now() {
  echo "Running once now with same command cron will use..."
  # shellcheck disable=SC2046
  bash -lc "$(build_job_command | sed -E 's/^[0-9\*].* //')"  # strip cron timing if any
  echo "Done."
}

# =========================
# Main
# =========================
case "${1:-}" in
  --help|-h) print_usage ;;
  --remove)  remove_cron_block; echo "Removed cron job '${CRON_TAG}'."; ;;
  --run)     run_once_now ;;
  *)         install_cron_block ;;
esac

