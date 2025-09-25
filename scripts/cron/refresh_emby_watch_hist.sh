#!/bin/sh

# This script adds/removes a cron job to the executing machine, that runs a DAILY fetch of the Emby
# 	user watch history and stores it into the local SQLite database.
# Under the hood, this runs a the "{SCRIPT NAME}.py 
