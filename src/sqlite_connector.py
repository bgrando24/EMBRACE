import sqlite3
import sys
from typing import Optional, Final
import os
from typing import Callable
from custom_types import T_EmbyAllUserWatchHist
from datetime import datetime, timedelta

class SQLiteConnector:
    # TODO: Consider placing table names into a class-global structure?
    
    """
    Provides functionality for interfacing with the app's SQLite database.
    
    NOTE: Fuctions prefixed with `_INIT_` are initialisation functions, they should only need to be ran at system start-up/creation.
    
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

    def _INIT_create_user_watch_hist_schemas(self) -> bool:
        """
        Creates (if doesn't exist) the full schema/tables for the user watch history data.
        
        NOTE: Requires the `library_items` table to exist. DB connection **must be active**.
        
        ---
        Tables: 
        
        `watch_hist_raw_events` (user watch hist verbatum from the Emby API)
        
        `watch_hist_agg_sessions` (Aggregated user sessions)
        
        `watch_hist_user_item_stats` (user-specific stats for their watched items)
        """

        # check db connection and cursor actually exist
        if self._connection is None:
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
# -----------------------
    
    def _INIT_DROP_user_watch_hist_schemas(self) -> bool:
        """
        **DROP** all user watch history tables

        ---
        Tables:
        `watch_hist_raw_events` - `watch_hist_agg_sessions` - `watch_hist_user_item_stats`
        """
        
        # check db connection and cursor actually exist
        if (self._connection is None):
            if self._debug: print(f"[SQliteConnector] ERROR: Database connection not found when attempting to create schema!", file=sys.stderr)
            return False
        if self._cursor is None:
            if self._debug: print(f"[SQliteConnector] ERROR: Database cursor not found when attempting to create schema!", file=sys.stderr)
            return False
        
        try:
            self._cursor.execute("DROP TABLE IF EXISTS watch_hist_raw_events")
            self._cursor.execute("DROP TABLE IF EXISTS watch_hist_agg_sessions")
            self._cursor.execute("DROP TABLE IF EXISTS watch_hist_user_item_stats")
            
            if self._debug: print("[SQLiteConnector] Watch history schemas DROPPED successfully!")
            return True
        except sqlite3.Error as e:
            if self._debug: print(f"[SQLiteConnector] ERROR: Failed to DROP user watch history schemas: {e}", file=sys.stderr)
            return False 
# -----------------------        
        
    def _INIT_POPULATE_watch_hist_raw_events(self, emby_watch_hist_func: Callable[[int, bool], T_EmbyAllUserWatchHist]) -> bool:
        """
        **WARNING: THIS WILL FIRST DROP ALL DATA IN THE `watch_hist_raw_events` TABLE**
        
        Populates the watch_hist_raw_events table rows with the all available historical data from the Emby API. 
        
        NOTE: handles the timezone cutoff where Playback Reporting switched from PDT to Melbourne time.
        
        Args:
            emby_watch_hist_func (() -> T_EmbyUserWatchHistResponse): Emby connector function that fetches all user watch history
        """
        
        if self._connection is None:
            if self._debug: print(f"[SQliteConnector] ERROR: Database connection not found!", file=sys.stderr)
            return False
        if self._cursor is None:
            if self._debug: print(f"[SQliteConnector] ERROR: Database cursor not found!", file=sys.stderr)
            return False
        
        # start fresh with clean table
        try:
            self._cursor.execute("DROP TABLE IF EXISTS watch_hist_raw_events")
            self._INIT_create_user_watch_hist_schemas()
        except sqlite3.Error as e:
            if self._debug: print(f"[SQLiteConnector] ERROR: Failed to DROP watch_hist_raw_events table: {e}", file=sys.stderr)
            return False 
        
        # when Emby watch history timezone was corrected from PDT to Melbourne (UTC+10)
        TIMEZONE_CUTOFF = datetime.strptime("2025-08-15 11:10:00", "%Y-%m-%d %H:%M:%S")
            
        if self._debug: 
            print("[SQLiteConnector] Fetching watch history from Emby for all users")
            print(f"[SQLiteConnector] Will adjust PDT timestamps before {TIMEZONE_CUTOFF} to Melbourne time (+17 hours)")
            
        
        # TODO: how to better handle the number of days of watch history to fetch? e.g. exp backfill
        # prep raw data for bulk insert -> transform into tuples for better insertion
        # see the watch_hist_raw_events schema for reference as to how the table is structured
        all_usr_hist = emby_watch_hist_func(2000, False)
        raw_events_data = []
        
        for username, data in all_usr_hist.items():
            for event in data:
                event_date = event["date"]
                event_time = event['time']
                
                # check if event is before cutoff
                event_datetime = datetime.strptime(f"{event_date} {event_time}", "%Y-%m-%d %H:%M:%S")
                
                # if before the cutoff, it's assumed PDT timezone
                if event_datetime < TIMEZONE_CUTOFF:
                    # + 17 hours to convert PDT (UTC-7) to Melbourne (UTC+10)
                    adjusted_datetime = event_datetime + timedelta(hours=17)
                    normalized_date = adjusted_datetime.strftime("%Y-%m-%d")
                    normalized_time = adjusted_datetime.strftime("%H:%M:%S")
                    
                    if self._debug and len(raw_events_data) < 3:  # Show first few adjustments
                        print(f"  Adjusted PDT→MEL: {event_date} {event_time} → {normalized_date} {normalized_time}")
                else:
                    # already in Melbourne time, use as-is
                    normalized_date = event_date
                    normalized_time = event_time
                
                raw_events_data.append([
                    normalized_date,
                    normalized_time,
                    event['user_id'],
                    event['item_name'],
                    event['item_id'],
                    event['item_type'],
                    int(event['duration']),
                    event.get('remote_address', ''),
                    event['user_name']
                ])
            
            if self._debug:
                print(f"  Found {len(data)} events for user: {username}")
        
        # bulk rows insert
        try:
            # don't wait for data to be fullly written to disk: https://www.sqlite.org/pragma.html#pragma_synchronous
            self._cursor.execute("PRAGMA synchronous = OFF")
            # store rollback journal in RAM instead of disk: https://www.sqlite.org/pragma.html#pragma_journal_mode
            self._cursor.execute("PRAGMA journal_mode = MEMORY")
            # make all inserts an atomic transaction (either all records succeed, or none)
            self._cursor.execute("BEGIN TRANSACTION")
            
            self._cursor.executemany(
                """INSERT OR IGNORE INTO watch_hist_raw_events 
                (date, time, user_id, item_name, item_id, item_type, 
                    duration, remote_address, user_name)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                raw_events_data
            )
            
            self._connection.commit()
            
            # reset pragmas
            self._cursor.execute("PRAGMA synchronous = NORMAL")
            self._cursor.execute("PRAGMA journal_mode = DELETE")
            
            if self._debug:
                print(f"[SQLiteConnector] Successfully inserted {len(raw_events_data)} watch events!")
                            
        except sqlite3.Error as e:
            print(f"[SQLiteConnector] ERROR during bulk insert: {e}", file=sys.stderr)
            self._connection.rollback()
            return False
        
        return True
# -----------------------


    def _INIT_POPULATE_watch_hist_agg_sessions(self, session_segment_minutes: int = 15) -> bool:
        """
        **WARNING: THIS WILL FIRST DROP ALL DATA IN THE `watch_hist_agg_sessions` TABLE**
        
        Populates the `watch_hist_agg_sessions` table rows with the all available data from the `watch_hist_raw_events` table.
        
        Args:
            session_segment_minutes (int, optional): Number of minutes to segment watch sessions into - DEFAULT: 15 minute sessions
        """
        
        if self._connection is None:
            if self._debug: print(f"[SQliteConnector] ERROR: Database connection not found!", file=sys.stderr)
            return False
        if self._cursor is None:
            if self._debug: print(f"[SQliteConnector] ERROR: Database cursor not found!", file=sys.stderr)
            return False
        
        # start fresh with clean table
        try:
            self._cursor.execute("DROP TABLE IF EXISTS watch_hist_agg_sessions")
            self._INIT_create_user_watch_hist_schemas()
        
            if self._debug:
                print(f"[SQLiteConnector] Processing raw events into sessions using {session_segment_minutes} minute segments")
                
            # convert session segment to fraction of a day for Julian day calculations: https://sqlite.org/lang_datefunc.html
            session_gap_days = session_segment_minutes / 1440.0  # 1440 minutes in a day
            
            # below query seems complex but here's a breakdown:
            # 1. ordered_events: Selects all raw events, formats timestamps, and orders them
            # 2. session_boundaries: Uses window functions to detect when a new session starts (gap > threshold)
            # 3. session_groups: Assigns a session group ID to each event by cumulatively summing new session markers
            # 4. Final SELECT: Aggregates events by session group, calculating session start/end, duration, total watch time, and outcome
            session_query = f"""
            INSERT INTO watch_hist_agg_sessions 
            (user_id, item_id, item_name, item_type, session_start_timestamp, 
            session_end_timestamp, session_duration_minutes, total_seconds_watched, 
            session_count, completion_ratio, outcome)
            WITH ordered_events AS (
                -- First, get all events with proper datetime formatting
                SELECT 
                    user_id,
                    item_id,
                    item_name,
                    item_type,
                    datetime(date || ' ' || time) as event_timestamp,
                    duration,
                    row_id
                FROM watch_hist_raw_events
                ORDER BY user_id, item_id, date, time
            ),
            session_boundaries AS (
                -- Identify session boundaries using LAG window function
                SELECT 
                    *,
                    LAG(event_timestamp) OVER (
                        PARTITION BY user_id, item_id 
                        ORDER BY event_timestamp
                    ) as prev_event_timestamp,
                    -- Check if gap from previous event > session_segment_minutes
                    CASE 
                        WHEN LAG(event_timestamp) OVER (
                            PARTITION BY user_id, item_id 
                            ORDER BY event_timestamp
                        ) IS NULL THEN 1  -- First event is always new session
                        WHEN julianday(event_timestamp) - julianday(
                            LAG(event_timestamp) OVER (
                                PARTITION BY user_id, item_id 
                                ORDER BY event_timestamp
                            )
                        ) > {session_gap_days} THEN 1  -- Gap > threshold means new session
                        ELSE 0
                    END as is_new_session
                FROM ordered_events
            ),
            session_groups AS (
                -- Assign session IDs using cumulative sum of new session markers
                SELECT 
                    *,
                    SUM(is_new_session) OVER (
                        PARTITION BY user_id, item_id 
                        ORDER BY event_timestamp
                        ROWS UNBOUNDED PRECEDING
                    ) as session_group_id
                FROM session_boundaries
            )
            -- Final aggregation by session
            SELECT 
                user_id,
                item_id,
                MAX(item_name) as item_name,  -- Should be same for all in group
                MAX(item_type) as item_type,
                MIN(event_timestamp) as session_start_timestamp,
                MAX(event_timestamp) as session_end_timestamp,
                CAST(
                    (julianday(MAX(event_timestamp)) - julianday(MIN(event_timestamp))) * 1440 
                    AS INTEGER
                ) as session_duration_minutes,
                SUM(duration) as total_seconds_watched,
                COUNT(*) as session_count,
                NULL as completion_ratio,  -- Will calculate later when we have item runtime data
                -- Determine outcome based on total watch time
                CASE 
                    WHEN MAX(item_type) = 'Episode' THEN
                        CASE
                            WHEN SUM(duration) >= 1200 THEN 'completed'  -- 20+ min for episodes
                            WHEN SUM(duration) >= 300 THEN 'partial'     -- 5-20 min
                            WHEN SUM(duration) >= 60 THEN 'sampled'      -- 1-5 min
                            ELSE 'abandoned'                              -- <1 min
                        END
                    WHEN MAX(item_type) = 'Movie' THEN
                        CASE
                            WHEN SUM(duration) >= 5400 THEN 'completed'  -- 90+ min for movies
                            WHEN SUM(duration) >= 1800 THEN 'partial'    -- 30-90 min
                            WHEN SUM(duration) >= 300 THEN 'sampled'     -- 5-30 min
                            ELSE 'abandoned'                              -- <5 min
                        END
                    ELSE 'unknown'
                END as outcome
            FROM session_groups
            GROUP BY user_id, item_id, session_group_id
            ORDER BY user_id, item_id, MIN(event_timestamp)
            """
            
            # Execute the session aggregation
            self._cursor.execute(session_query)
            rows_inserted = self._cursor.rowcount
            self._connection.commit()
            
            if self._debug:
                print(f"[SQLiteConnector] Successfully created {rows_inserted} aggregated sessions!")
                
                # Print some statistics
                self._cursor.execute("""
                    SELECT 
                        COUNT(DISTINCT user_id) as unique_users,
                        COUNT(DISTINCT item_id) as unique_items,
                        COUNT(*) as total_sessions,
                        AVG(session_duration_minutes) as avg_session_minutes,
                        AVG(total_seconds_watched/60.0) as avg_watch_minutes
                    FROM watch_hist_agg_sessions
                """)
                stats = self._cursor.fetchone()
                if stats:
                    print(f"  - Unique users: {stats[0]}")
                    print(f"  - Unique items: {stats[1]}")
                    print(f"  - Total sessions: {stats[2]}")
                    print(f"  - Avg session duration: {stats[3]:.1f} minutes")
                    print(f"  - Avg watch time per session: {stats[4]:.1f} minutes")
                    
                    # Show outcome distribution
                    self._cursor.execute("""
                        SELECT outcome, COUNT(*) as count
                        FROM watch_hist_agg_sessions
                        GROUP BY outcome
                        ORDER BY count DESC
                    """)
                    print("  - Session outcomes:")
                    for outcome, count in self._cursor.fetchall():
                        print(f"    * {outcome}: {count}")
        
        except sqlite3.Error as e:
            if self._debug: print(f"[SQLiteConnector] ERROR: When attempting to create aggregate watch session rows: {e}", file=sys.stderr)
            self._connection.rollback()
            return False 

        return True