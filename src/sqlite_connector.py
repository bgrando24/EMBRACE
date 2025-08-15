import sqlite3
import sys
from typing import Optional, Final
import os

class SQLiteConnector:
    """
    Provides functionality for interfacing with the app's SQLite database
    
    Attributes:
        _connection (Optional[sqlite3.Connection]): The database connection object. Method 'connect_db()' must be executed to obtain this object.
        cursor (sqlite3.Cursor): DB cursor used to execute queries. Method 'connect_db()' must be executed to set the cursor.
    """
    _cursor: Optional[sqlite3.Cursor]
    _connection: Optional[sqlite3.Connection]

    def __init__(self, DB_NAME: str, debug=False):
        self._debug = debug
        
        if DB_NAME is None or DB_NAME == '':
            print("[SQliteConnector] ERROR: DB_NAME environment variable is not set! Check the .env file at the root of the project.", file=sys.stderr)
            exit(1)
        os.makedirs("sqlite_db", exist_ok=True)    # hardcoded on purpose to enforce consistent directories, also helps manage gitignore if DB name is different from default
        self.__DB_DIR: Final = os.path.join("sqlite_db", DB_NAME)
        
    def connect_db(self) -> bool:
        """
        Attempts to connect to the DB, and grab both the cursor and connector objects
        
        Returns:
            bool: True if successful, False if not
        """
        try:
            self._connection = sqlite3.connect(self.__DB_DIR)
            # check db is actually connected and reachable
            self._connection.execute("SELECT 1;")
            if self._debug: print("DB connection tested and successful!")
        except sqlite3.Error as e:
            print(f"[SQliteConnector] ERROR: Database connection failed: {e}", file=sys.stderr)
            self._connection = None
            exit(1)
            return False
        # grab cursor, necessary for running queries
        self._cursor = self._connection.cursor()
        return True
    
    
    # ================================== Setup methods ==================================

    def INIT_create_user_watch_hist_schemas(self) -> bool:
        """
        Creates (if doesn't exist) the full schema/tables for the user watch history data.
        
        NOTE: Requires the `library_items` table to exist. DB connection **must be active**.
        
        ---
        Tables: 
        
        `watch_hist_raw_events` (user watch his verbatum from the Emby API)
        
        `watch_hist_agg_sessions` (Aggregated user sessions)
        
        `watch_hist_user_item_stats` (user-specific stats for their watched items)
        """

        # check db connection and cursor actually exist
        if (self._connection is None):
            if self._debug: print(f"[SQliteConnector] ERROR: Database connection not found when attempting to create schema!", file=sys.stderr)
            return False
        if self._cursor is None:
            if self._debug: print(f"[SQliteConnector] ERROR: Database cursor not found when attempting to create schema!", file=sys.stderr)
            return False

        # TABLE: watch_hist_raw_events
        SCHEMA_watch_hist_raw_events = """
            CREATE TABLE IF NOT EXISTS watch_hist_raw_events(
                row_id INTEGER PRIMARY KEY AUTOINCREMENT, 
                date TEXT NOT NULL, 
                time TEXT NOT NULL, 
                user_id TEXT NOT NULL,
                item_name TEXT NOT NULL,
                item_id INTEGER NOT NULL,
                item_type TEXT NOT NULL,
                duration INTEGER NOT NULL,
                remote_address TEXT,
                user_name TEXT NOT NULL
            )"""
        
        #TABLE: watch_hist_agg_sessions
        SCHEMA_watch_hist_agg_sessions = """
            CREATE TABLE IF NOT EXISTS watch_hist_agg_sessions(
                session_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                item_id INTEGER NOT NULL,
                item_name TEXT NOT NULL,
                item_type TEXT NOT NULL,
                session_start_timestamp TEXT NOT NULL,
                session_end_timestamp TEXT NOT NULL,
                session_duration_minutes INTEGER,
                total_seconds_watched INTEGER NOT NULL,
                session_count INTEGER NOT NULL,
                completion_ratio REAL,
                outcome TEXT,
                created_timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, item_id, session_start_timestamp)
            )
        """
        
        #TABLE: watch_hist_user_item_stats
        SCHEMA_watch_hist_user_item_stats = """
            CREATE TABLE IF NOT EXISTS watch_hist_user_item_stats (
            stat_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            item_id INTEGER NOT NULL,
            item_name TEXT NOT NULL,
            item_type TEXT NOT NULL,
            total_sessions INTEGER NOT NULL DEFAULT 0,
            total_seconds_watched INTEGER NOT NULL DEFAULT 0,
            total_minutes_watched REAL GENERATED ALWAYS AS (total_seconds_watched / 60.0),
            best_completion_ratio REAL DEFAULT 0,
            average_completion_ratio REAL DEFAULT 0,
            rewatch_count INTEGER DEFAULT 0,
            first_watched_timestamp TEXT,
            last_watched_timestamp TEXT,
            days_between_first_last INTEGER GENERATED ALWAYS AS (
                CASE 
                    WHEN first_watched_timestamp = last_watched_timestamp THEN 0
                    ELSE JULIANDAY(last_watched_timestamp) - JULIANDAY(first_watched_timestamp)
                END
            ),
            adherence_score REAL DEFAULT 0,
            completed_sessions INTEGER DEFAULT 0,
            partial_sessions INTEGER DEFAULT 0,
            abandoned_sessions INTEGER DEFAULT 0,
            sampled_sessions INTEGER DEFAULT 0,
            last_updated_timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, item_id)
            )
        """
        try:
            # create tables
            self._cursor.execute(SCHEMA_watch_hist_raw_events)
            self._cursor.execute(SCHEMA_watch_hist_agg_sessions)
            self._cursor.execute(SCHEMA_watch_hist_user_item_stats)
            
            # indexes for each table
            self._cursor.execute("CREATE INDEX IF NOT EXISTS idx_watch_hist_raw_user_time ON watch_hist_raw_events(user_id, date, time DESC)")
            self._cursor.execute("CREATE INDEX IF NOT EXISTS idx_watch_hist_agg_sessions ON watch_hist_agg_sessions(user_id, session_end_timestamp DESC)")
            self._cursor.execute("CREATE INDEX IF NOT EXISTS idx_watch_hist_user_item_stats ON watch_hist_user_item_stats(user_id, adherence_score DESC)")
            
            self._connection.commit()
            
            if self._debug: print("[SQLiteConnector] Watch history schemas created successfully!")
            return True
            
        except sqlite3.Error as e:
            if self._debug: print(f"[SQLiteConnector] ERROR: Failed to create user watch history schemas schema: {e}", file=sys.stderr)
            return False
        