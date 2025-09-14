import pandas as pd
import sqlite3
import sys
from typing import Final, List, Dict, Tuple
from collections import defaultdict

from custom_types import T_EmbyWatchHistStatsRow

class Analytics:
    """Analytics helper class for functionality related to viewing, analysing, and manipulating SQL data within our python code"""
    
    def __init__(self, SQLITE_DB_CONNECTOR: sqlite3.Connection) -> None:
        if SQLITE_DB_CONNECTOR is None:
            print("[Analytics] ERROR: SQLITE_DB_CURSOR variable is not set at class instantiation!", file=sys.stderr)
            exit(1)
        self.__connector: Final = SQLITE_DB_CONNECTOR
        self.__cursor: Final = SQLITE_DB_CONNECTOR.cursor()
    
    def get_user_item_stats(self) -> List[T_EmbyWatchHistStatsRow]:
        """
        Generates a statistics view for each item, per user.
        
        Stats: num_plays, total_minutes_watched, avg_fraction_completed, first_watch_at, last_watch_at
        """
        # load data from DB, compute into dataframes
        self.__cursor.execute("SELECT * FROM watch_hist_user_item_stats")
        db_results: List[T_EmbyWatchHistStatsRow] = self.__cursor.fetchall()
        self.__connector.commit()
        # return db_results

        # FOR REFERENCE:
        # int,    # stat_id
        # str,    # user_id
        # str,    # item_id
        # int,    # total_sessions
        # int,    # total_seconds_watched
        # float,  # total_minutes_watched
        # float,  # best_completion_ratio
        # float,  # average_completion_ratio
        # int,    # rewatch_count
        # str,    # first_watched_timestamp
        # str,    # last_watched_timestamp
        # float,  # days_between_first_last
        # float,  # adherence_score
        # int,    # completed_sessions
        # int,    # partial_sessions
        # int,    # abandoned_sessions
        # int,    # sampled_sessions
        # str     # last_updated_timestamp

        # load data into dataframe, rename column headers, calculate stats as above
        df = pd.DataFrame(db_results)
        df.rename(columns={
            0: "stat_id",
            1: "user_id",
            2: "item_id",
            3: "total_sessions",
            4: "total_seconds_watched",
            5: "total_minutes_watched",
            6: "best_completion_ratio",
            7: "average_completion_ratio",
            8: "rewatch_count",
            9: "first_watched_timestamp",
            10: "last_watched_timestamp",
            11: "days_between_first_last",
            12: "adherence_score",
            13: "completed_sessions",
            14: "partial_sessions",
            15: "abandoned_sessions",
            16: "sampled_sessions",
            17: "last_updated_timestamp"
        }, inplace=True)
        
        