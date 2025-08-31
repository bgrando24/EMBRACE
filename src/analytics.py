import pandas as pd
import sqlite3
import sys
from typing import Final

class Analytics:
    """Analytics helper class for functionality related to viewing, analysing, and manipulating SQL data within our python code"""
    
    def __init__(self, SQLITE_DB_CONNECTOR: sqlite3.Connection) -> None:
        if SQLITE_DB_CONNECTOR is None:
            print("[Analytics] ERROR: SQLITE_DB_CURSOR variable is not set at class instantiation!", file=sys.stderr)
            exit(1)
        self.__connector: Final = SQLITE_DB_CONNECTOR
        self.__cursor: Final = SQLITE_DB_CONNECTOR.cursor()
    
    def get_user_item_stats(self):
        """
        Generates a statistics view for each item, per user.
        
        Stats: num_plays, total_minutes_watched, avg_fraction_completed, first_watch_at, last_watch_at
        """
        # load data from DB, compute into dataframes
        self.__cursor.execute("SELECT * FROM watch_hist_user_item_stats")
        db_results = self.__cursor.fetchall()
        self.__connector.commit()
        return db_results
        