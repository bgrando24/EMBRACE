import pandas as pd
import sqlite3
import sys
from typing import Final, List, Dict, Tuple
from collections import defaultdict

from custom_types import T_EmbyWatchHistStatsRow
from emby_connector import Emby

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
        Generates a statistics view per user for each applicable item.
        
        Stats: num_plays, total_minutes_watched, avg_fraction_completed, first_watch_at, last_watch_at
        """
        # load data from DB, compute into dataframe
        self.__cursor.execute("SELECT * FROM watch_hist_user_item_stats")
        db_results: List[T_EmbyWatchHistStatsRow] = self.__cursor.fetchall()
        self.__connector.commit()

        # grab users from Emby connector
        

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
        
        # print(df["total_sessions"].mean())

        # match items and users to string values based on IDs
        items: Dict[str, str] = {}
        users: Dict[str, str] = {}

        # build output into here
        stats_df = pd.DataFrame
        for row in df.itertuples(False, None): 
            # print(row)
            # print("\n")
            # example 'row' iteration:
            # (
            #   465, 
            #   'f49c1281cadb43499181b8759f6ae81b', 
            #   '536641', 
            #   1, 
            #   7458, 
            #   124.3, 
            #   0.9608348363823757, 
            #   0.9608348363823757, 
            #   0, 
            #   '2025-08-22 21:53:57', 
            #  '2025-08-22 21:53:57', 
            #   0.0, 
            #   0.772584385467663, 
            #   1, 
            #   0, 
            #   0, 
            #   0, 
            #   '2025-09-02 09:54:16'
            # )
            
            users.update({row[1]})