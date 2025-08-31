from itertools import islice
import sqlite3
import sys
from typing import Optional, Final
import os
from typing import Callable, Dict
import zlib
from custom_types import T_EmbyAllUserWatchHist, T_TMDBGenres
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
    
    def _extract_video_codec(self, item_data: dict) -> str:
        """
        Extract video codec from MediaStreams
        """
        for stream in item_data.get('MediaStreams', []):
            if stream.get('Type') == 'Video':
                return stream.get('Codec', '')
        return ''
    
    def prune_missing_items(self, current_ids: set[str]) -> int:
        """
        Delete library rows not in current_ids. Returns rows deleted, or -1 if error. 
        """
        if self._connection is None or self._cursor is None:
            print("[SQLiteConnector] ERROR: DB not connected", file=sys.stderr)
            return -1

        # Coerce to str to avoid int-vs-str mismatches
        current_ids = {str(i) for i in current_ids}

        self._connection.execute("BEGIN")
        self._cursor.execute("SELECT item_id FROM library_items")
        existing = {str(row[0]) for row in self._cursor.fetchall()}

        to_delete = existing - current_ids
        if to_delete:
            # Optional: chunk to avoid huge parameter lists
            def chunks(iterable, size=500):
                it = iter(iterable)
                while True:
                    batch = list(islice(it, size))
                    if not batch:
                        break
                    yield batch

            for batch in chunks(to_delete, 500):
                self._cursor.executemany("DELETE FROM item_provider_ids WHERE item_id = ?", [(i,) for i in batch])
                self._cursor.executemany("DELETE FROM item_genres       WHERE item_id = ?", [(i,) for i in batch])
                self._cursor.executemany("DELETE FROM item_tags         WHERE item_id = ?", [(i,) for i in batch])
                self._cursor.executemany("DELETE FROM library_items     WHERE item_id = ?", [(i,) for i in batch])

        self._connection.commit()
        return len(to_delete)

    
    
    # ====================================================================== User Watch History Tables ======================================================================

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
                item_id TEXT NOT NULL,
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
                item_id TEXT NOT NULL,
                session_start_timestamp TEXT NOT NULL,
                session_end_timestamp TEXT NOT NULL,
                session_span_minutes INTEGER,
                total_seconds_watched INTEGER NOT NULL,
                session_count INTEGER NOT NULL,
                completion_ratio REAL,
                outcome TEXT,
                created_timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, item_id, session_start_timestamp),
                FOREIGN KEY (item_id) REFERENCES library_items(item_id)
            )
        """
        
        #TABLE: watch_hist_user_item_stats
        SCHEMA_watch_hist_user_item_stats = """
            CREATE TABLE IF NOT EXISTS watch_hist_user_item_stats (
                stat_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                item_id TEXT NOT NULL,
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
                UNIQUE(user_id, item_id),
                FOREIGN KEY (item_id) REFERENCES library_items(item_id)
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
            self._cursor.execute("CREATE INDEX IF NOT EXISTS idx_watch_hist_agg_item ON watch_hist_agg_sessions(item_id)")
            self._cursor.execute("CREATE INDEX IF NOT EXISTS idx_watch_hist_user_item_stats ON watch_hist_user_item_stats(user_id, adherence_score DESC)")
            
            self._connection.commit()
            
            if self._debug: print("[SQLiteConnector] Watch history schemas created successfully!")
            
        except sqlite3.Error as e:
            if self._debug: print(f"[SQLiteConnector] ERROR: Failed to create user watch history schemas: {e}", file=sys.stderr)
            return False    
        
        return True
# ----------------------------------

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


    def _INIT_POPULATE_watch_hist_agg_sessions(
        self,
        session_segment_minutes: int = 15,
        completed_ratio_threshold: float = 0.9,
        partial_ratio_threshold: float = 0.25,
        min_sampled_seconds: int = 60,
    ) -> bool:
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
            completed_t = float(completed_ratio_threshold)
            partial_t = float(partial_ratio_threshold)
            min_sample = int(min_sampled_seconds)
            
            # below query seems complex but here's a breakdown:
            # 1. ordered_events: Selects all raw events, formats timestamps, and orders them
            # 2. session_boundaries: Uses window functions to detect when a new session starts (gap > threshold)
            # 3. session_groups: Assigns a session group ID to each event by cumulatively summing new session markers
            # 4. Final SELECT: Aggregates events by session group, calculating session start/end, duration, total watch time, and outcome
            session_query = f"""
                INSERT INTO watch_hist_agg_sessions 
                (user_id, item_id, session_start_timestamp, 
                session_end_timestamp, session_span_minutes, total_seconds_watched, 
                session_count, completion_ratio, outcome)
                WITH ordered_events AS (
                    -- First, get all events with proper datetime formatting
                    SELECT 
                        user_id,
                        item_id,
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
                    MIN(event_timestamp) as session_start_timestamp,
                    MAX(event_timestamp) as session_end_timestamp,
                    CAST(
                        (julianday(MAX(event_timestamp)) - julianday(MIN(event_timestamp))) * 1440 
                        AS INTEGER
                    ) as session_span_minutes,
                    SUM(duration) as total_seconds_watched,
                    COUNT(*) as session_count,
                    -- Compute completion ratio using actual runtime when available
                    CASE 
                        WHEN (
                            SELECT runtime_seconds 
                            FROM library_items 
                            WHERE item_id = session_groups.item_id
                        ) > 0 THEN 
                            MIN(
                                1.0, 
                                SUM(duration) / CAST((
                                    SELECT runtime_seconds 
                                    FROM library_items 
                                    WHERE item_id = session_groups.item_id
                                ) AS REAL)
                            )
                        WHEN MAX(item_type) = 'Episode' THEN 
                            MIN(1.0, SUM(duration) / 1500.0)  -- Fallback 25 min
                        WHEN MAX(item_type) = 'Movie' THEN   
                            MIN(1.0, SUM(duration) / 7200.0)  -- Fallback 2 hours
                        ELSE NULL
                    END as completion_ratio,
                    -- Determine outcome using actual runtime when available
                    CASE 
                        WHEN (
                            SELECT runtime_seconds 
                            FROM library_items 
                            WHERE item_id = session_groups.item_id
                        ) > 0 THEN 
                            CASE
                                WHEN SUM(duration) >= {completed_t} * (
                                    SELECT runtime_seconds 
                                    FROM library_items 
                                    WHERE item_id = session_groups.item_id
                                ) THEN 'completed'
                                WHEN SUM(duration) >= {partial_t} * (
                                    SELECT runtime_seconds 
                                    FROM library_items 
                                    WHERE item_id = session_groups.item_id
                                ) THEN 'partial'
                                WHEN SUM(duration) >= {min_sample} THEN 'sampled'
                                ELSE 'abandoned'
                            END
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
                        AVG(session_span_minutes) as avg_session_minutes,
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
# -----------------------


    def _INIT_POPULATE_watch_hist_user_item_stats(self) -> bool:
        """
        **WARNING: THIS WILL FIRST DROP ALL DATA IN THE `watch_hist_user_item_stats` TABLE**
        
        Populates the user-item statistics table by aggregating data from watch_hist_agg_sessions.
        """
        
        if self._connection is None:
            if self._debug: print(f"[SQliteConnector] ERROR: Database connection not found!", file=sys.stderr)
            return False
        if self._cursor is None:
            if self._debug: print(f"[SQliteConnector] ERROR: Database cursor not found!", file=sys.stderr)
            return False
        
        try:
            # clear out table
            self._cursor.execute("DROP TABLE IF EXISTS watch_hist_user_item_stats")
            self._INIT_create_user_watch_hist_schemas()
            
            if self._debug:
                print("[SQLiteConnector] Calculating user-item statistics from aggregated sessions")
            
            stats_query = """
                INSERT INTO watch_hist_user_item_stats
                (user_id, item_id, total_sessions, total_seconds_watched,
                best_completion_ratio, average_completion_ratio,
                rewatch_count, first_watched_timestamp, last_watched_timestamp,
                adherence_score, completed_sessions, partial_sessions, 
                abandoned_sessions, sampled_sessions)
                SELECT 
                    s.user_id,
                    s.item_id,
                    COUNT(*) as total_sessions,
                    SUM(s.total_seconds_watched) as total_seconds,
                    -- Use actual runtime from library_items when available
                    CASE
                        WHEN l.runtime_seconds > 0 THEN
                            MIN(1.0, MAX(s.total_seconds_watched) / CAST(l.runtime_seconds AS REAL))
                        WHEN l.item_type = 'Episode' THEN
                            MIN(1.0, MAX(s.total_seconds_watched) / 1500.0)  -- Fallback 25 min
                        WHEN l.item_type = 'Movie' THEN  
                            MIN(1.0, MAX(s.total_seconds_watched) / 7200.0)  -- Fallback 2 hours
                        ELSE 0
                    END as best_completion,
                    CASE
                        WHEN l.runtime_seconds > 0 THEN
                            MIN(1.0, AVG(s.total_seconds_watched) / CAST(l.runtime_seconds AS REAL))
                        WHEN l.item_type = 'Episode' THEN
                            MIN(1.0, AVG(s.total_seconds_watched) / 1500.0)
                        WHEN l.item_type = 'Movie' THEN
                            MIN(1.0, AVG(s.total_seconds_watched) / 7200.0)
                        ELSE 0
                    END as avg_completion,
                    -- Rewatch count (sessions beyond the first)
                    CASE 
                        WHEN COUNT(*) > 1 THEN COUNT(*) - 1 
                        ELSE 0 
                    END as rewatches,
                    MIN(s.session_start_timestamp) as first_watched,
                    MAX(s.session_end_timestamp) as last_watched,
                    -- Adherence score using actual runtime when available
                    CASE
                        WHEN l.runtime_seconds > 0 THEN
                            (0.6 * MIN(1.0, MAX(s.total_seconds_watched) / CAST(l.runtime_seconds AS REAL)) +
                            0.3 * MIN(1.0, COUNT(*) / 3.0) +
                            0.1 * MIN(1.0, SUM(s.total_seconds_watched) / CAST(l.runtime_seconds AS REAL)))
                        WHEN l.item_type = 'Episode' THEN
                            (0.6 * MIN(1.0, MAX(s.total_seconds_watched) / 1500.0) +
                            0.3 * MIN(1.0, COUNT(*) / 3.0) +
                            0.1 * MIN(1.0, SUM(s.total_seconds_watched) / 3600.0))
                        WHEN l.item_type = 'Movie' THEN
                            (0.7 * MIN(1.0, MAX(s.total_seconds_watched) / 7200.0) +
                            0.2 * MIN(1.0, COUNT(*) / 2.0) +
                            0.1 * MIN(1.0, SUM(s.total_seconds_watched) / 7200.0))
                        ELSE 0
                    END as adherence,
                    -- Session outcome counts
                    SUM(CASE WHEN s.outcome = 'completed' THEN 1 ELSE 0 END) as completed,
                    SUM(CASE WHEN s.outcome = 'partial' THEN 1 ELSE 0 END) as partial,
                    SUM(CASE WHEN s.outcome = 'abandoned' THEN 1 ELSE 0 END) as abandoned,
                    SUM(CASE WHEN s.outcome = 'sampled' THEN 1 ELSE 0 END) as sampled
                FROM watch_hist_agg_sessions s
                LEFT JOIN library_items l ON s.item_id = l.item_id
                GROUP BY s.user_id, s.item_id
            """
            
            self._cursor.execute(stats_query)
            rows_inserted = self._cursor.rowcount
            self._connection.commit()
            
            if self._debug:
                print(f"[SQLiteConnector] Successfully created stats for {rows_inserted} user-item pairs")
            
        except sqlite3.Error as e:
            if self._debug:
                print(f"[SQLiteConnector] ERROR creating user-item stats: {e}", file=sys.stderr)
            self._connection.rollback()
            return False
        
        return True
# -------------------------------------------

    def update_completion_ratios(self):
        """
        Update completion ratios in sessions table using actual runtime data
        """
        if self._connection is None:
            if self._debug: print(f"[SQliteConnector] ERROR: Database connection not found!", file=sys.stderr)
            return False
        if self._cursor is None:
            if self._debug: print(f"[SQliteConnector] ERROR: Database cursor not found!", file=sys.stderr)
            return False
        
        # SQLite-compatible correlated subquery (no UPDATE ... FROM support)
        self._cursor.execute(
            """
            UPDATE watch_hist_agg_sessions
            SET completion_ratio = MIN(
                1.0,
                CAST(total_seconds_watched AS REAL) / (
                    SELECT runtime_seconds 
                    FROM library_items l
                    WHERE l.item_id = watch_hist_agg_sessions.item_id
                )
            )
            WHERE EXISTS (
                SELECT 1 FROM library_items l
                WHERE l.item_id = watch_hist_agg_sessions.item_id
                AND l.runtime_seconds > 0
            )
            """
        )
        
        self._connection.commit()
        
        if self._debug:
            print("[SQLiteConnector] Updated completion ratios with actual runtime data")
# -------------------------------------------
    

    # ====================================================================== Emby Library Tables ======================================================================

    def _INIT_create_library_items_schema(self) -> bool:
        """
        Creates the library_items table for movie/episode metadata
        """
        
        if self._connection is None:
            if self._debug: print(f"[SQliteConnector] ERROR: Database connection not found when attempting to create schema!", file=sys.stderr)
            return False
        if self._cursor is None:
            if self._debug: print(f"[SQliteConnector] ERROR: Database cursor not found when attempting to create schema!", file=sys.stderr)
            return False
        
        SCHEMA_library_items = """
            CREATE TABLE IF NOT EXISTS library_items (
                item_id TEXT PRIMARY KEY,
                item_name TEXT NOT NULL,
                item_type TEXT NOT NULL,  -- 'Episode' or 'Movie'
                
                -- Episode-specific fields
                series_name TEXT,
                series_id TEXT,
                season_number INTEGER,
                episode_number INTEGER,
                
                -- Runtime and dates
                runtime_ticks BIGINT,  -- Emby uses ticks (10,000,000 = 1 second)
                runtime_seconds INTEGER GENERATED ALWAYS AS (runtime_ticks / 10000000),
                runtime_minutes REAL GENERATED ALWAYS AS (runtime_ticks / 600000000.0),
                premiere_date TEXT,
                date_created TEXT,
                
                -- Content metadata
                overview TEXT,
                community_rating REAL,
                production_year INTEGER,
                
                -- File info
                file_path TEXT,
                container TEXT,
                video_codec TEXT,
                resolution_width INTEGER,
                resolution_height INTEGER,
                
                -- Timestamps
                last_updated TEXT DEFAULT CURRENT_TIMESTAMP,
                
                UNIQUE(item_id)
            )
        """
        
        # separate tables for many-to-many relationships
        SCHEMA_item_genres = """
            CREATE TABLE IF NOT EXISTS item_genres (
                uuid INTEGER PRIMARY KEY AUTOINCREMENT,
                item_id TEXT,
                genre_id INTEGER,
                genre_name TEXT,
                FOREIGN KEY (item_id) REFERENCES library_items(item_id)
            )
        """
        
        SCHEMA_item_tags = """
            CREATE TABLE IF NOT EXISTS item_tags (
                item_id TEXT,
                tag_id INTEGER,
                tag_name TEXT,
                PRIMARY KEY (item_id, tag_id),
                FOREIGN KEY (item_id) REFERENCES library_items(item_id)
            )
        """
        
        # will be used later
        # SCHEMA_item_metadata = """
        # CREATE TABLE IF NOT EXISTS item_enriched_metadata (
        #     item_id TEXT PRIMARY KEY,
        #     content_tags TEXT,  -- JSON array of detailed tags
        #     themes TEXT,        -- JSON array of themes
        #     style_attributes TEXT,  -- JSON array of style descriptors
        #     embedding BLOB,     -- Vector embedding for similarity
        #     metadata_source TEXT,  -- 'tmdb', 'manual', 'llm_generated', etc.
        #     last_updated TEXT DEFAULT CURRENT_TIMESTAMP,
        #     FOREIGN KEY (item_id) REFERENCES library_items(item_id)
        # )
        # """
        
        try:
            self._cursor.execute(SCHEMA_library_items)
            self._cursor.execute(SCHEMA_item_genres)
            self._cursor.execute(SCHEMA_item_tags)
            
            # indexes
            self._cursor.execute("CREATE INDEX IF NOT EXISTS idx_library_series ON library_items(series_id)")
            self._cursor.execute("CREATE INDEX IF NOT EXISTS idx_library_type ON library_items(item_type)")
            self._cursor.execute("CREATE INDEX IF NOT EXISTS idx_series_season_ep ON library_items(series_id, season_number, episode_number)")
            self._cursor.execute("CREATE INDEX IF NOT EXISTS idx_library_name ON library_items(item_name)")
            self._cursor.execute("CREATE INDEX IF NOT EXISTS idx_library_year ON library_items(production_year)")
            self._cursor.execute("CREATE INDEX IF NOT EXISTS idx_genres_item ON item_genres(item_id)")
            self._cursor.execute("CREATE INDEX IF NOT EXISTS idx_tags_item ON item_tags(item_id)")
            
        except sqlite3.Error as e:
            if self._debug: print(f"[SQLiteConnector] ERROR: Failed to create library item schemas: {e}", file=sys.stderr)
            return False 
        
        return True
# ----------------------------------

    
    def _ensure_provider_ids_schema(self):
        sql = """
        CREATE TABLE IF NOT EXISTS item_provider_ids(
            item_id TEXT NOT NULL,
            provider TEXT NOT NULL,
            provider_item_id TEXT NOT NULL,
            PRIMARY KEY (item_id, provider),
            FOREIGN KEY (item_id) REFERENCES library_items(item_id)
        );
        """
        if self._connection is None or self._cursor is None:
            if self._debug: print("[SQLiteConnector] ERROR: DB not connected", file=sys.stderr)
            return False
        
        self._cursor.execute(sql)
        self._cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_provider_provider ON item_provider_ids(provider)"
        )
# -------------------------------------------


    def ingest_all_library_items(self, emby_items_iterable, get_item_metadata: Optional[Callable[[str], dict]] = None) -> bool:
        """
        Ingest EVERY Movie/Episode from Emby into library_items (+ genres/tags + provider ids).
        emby_items_iterable should yield BaseItemDto dicts (see EmbyConnector.iter_all_items()).
        Optionally provide get_item_metadata(item_id: str) to enable series-level genre fallback for Episodes.
        """
        if self._connection is None or self._cursor is None:
            print("[SQLiteConnector] ERROR: DB not connected", file=sys.stderr)
            return False

        # check schemas exist
        if not self._INIT_create_library_items_schema():
            return False
        self._ensure_provider_ids_schema()

        # upserts
        upsert_item_sql = """
        INSERT OR REPLACE INTO library_items(
            item_id, item_name, item_type,
            series_name, series_id, season_number, episode_number,
            runtime_ticks, premiere_date, overview, community_rating, production_year,
            file_path, container, video_codec, resolution_width, resolution_height
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """

        upsert_genre_sql = """
        INSERT OR REPLACE INTO item_genres(item_id, genre_id, genre_name)
        VALUES(?,?,?)
        """

        upsert_tag_sql = """
        INSERT OR REPLACE INTO item_tags(item_id, tag_id, tag_name)
        VALUES(?,?,?)
        """

        upsert_provider_sql = """
        INSERT OR REPLACE INTO item_provider_ids(item_id, provider, provider_item_id)
        VALUES(?,?,?)
        """

        cur = self._cursor

        # Preload TMDB genre name->id maps for fast lookup
        movie_genre_map: Dict[str, int] = {}
        tv_genre_map: Dict[str, int] = {}
        try:
            for gid, gname in cur.execute("SELECT id, name FROM tmdb_movie_genres"):
                movie_genre_map[str(gname).lower()] = int(gid)
        except sqlite3.Error:
            movie_genre_map = {}
        try:
            for gid, gname in cur.execute("SELECT id, name FROM tmdb_tv_genres"):
                tv_genre_map[str(gname).lower()] = int(gid)
        except sqlite3.Error:
            tv_genre_map = {}

        def _stable_id_from_name(name: str) -> int:
            # Deterministic non-negative id from name
            return int(zlib.crc32(name.lower().encode("utf-8")) & 0x7FFFFFFF)

        # Cache for series metadata lookups to avoid repeated API calls
        series_meta_cache: Dict[str, dict] = {}
        try:
            self._connection.execute("BEGIN")
            batch = 0
            seen = set()    # for pruning items no longer in library

            for item in emby_items_iterable:
                item_id = str(item.get("Id"))
                seen.add(item_id)
                # Width/Height may come on item OR video stream; use item first, then fallback
                width = item.get("Width")
                height = item.get("Height")
                if (width is None or height is None) and item.get("MediaStreams"):
                    for s in item["MediaStreams"]:
                        if s.get("Type") == "Video":
                            width = width or s.get("Width")
                            height = height or s.get("Height")
                            break

                cur.execute(
                    upsert_item_sql,
                    (
                        item_id,
                        item.get("Name"),
                        item.get("Type"),
                        item.get("SeriesName"),
                        item.get("SeriesId"),
                        item.get("ParentIndexNumber"),   # season number
                        item.get("IndexNumber"),         # episode number
                        item.get("RunTimeTicks"),
                        item.get("PremiereDate"),
                        item.get("Overview"),
                        item.get("CommunityRating"),
                        item.get("ProductionYear"),
                        item.get("Path"),
                        item.get("Container"),
                        self._extract_video_codec(item),
                        width, height
                    )
                )

                # genres (prefer object form; fallback to name list and TMDB mapping)
                added_genre_names = set()
                for g in item.get("GenreItems", []) or []:
                    gname = g.get("Name")
                    gid = g.get("Id")
                    if gname:
                        added_genre_names.add(gname)
                    cur.execute(upsert_genre_sql, (item_id, gid, gname))

                # Fallback to simple string list if present and not already added
                for gname in item.get("Genres", []) or []:
                    if not gname or gname in added_genre_names:
                        continue
                    key = gname.lower()
                    itype = item.get("Type")
                    # Choose appropriate TMDB map by item type; fallback to other if needed
                    gid = None
                    if itype == "Movie":
                        gid = movie_genre_map.get(key) or tv_genre_map.get(key)
                    elif itype == "Episode":
                        gid = tv_genre_map.get(key) or movie_genre_map.get(key)
                    else:
                        gid = movie_genre_map.get(key) or tv_genre_map.get(key)
                    if gid is None:
                        gid = _stable_id_from_name(gname)
                    cur.execute(upsert_genre_sql, (item_id, gid, gname))
                    added_genre_names.add(gname)

                # If still no genres on an Episode, fall back to its Series metadata
                if not added_genre_names and (item.get("Type") == "Episode") and get_item_metadata:
                    series_id = item.get("SeriesId")
                    if series_id:
                        sid = str(series_id)
                        series_meta = series_meta_cache.get(sid)
                        if series_meta is None:
                            try:
                                series_meta = get_item_metadata(sid) or {}
                            except Exception as _e:
                                series_meta = {}
                            series_meta_cache[sid] = series_meta

                        # Use Series GenreItems if available
                        for g in series_meta.get("GenreItems", []) or []:
                            gname = g.get("Name")
                            gid = g.get("Id")
                            if gname and gname not in added_genre_names:
                                cur.execute(upsert_genre_sql, (item_id, gid, gname))
                                added_genre_names.add(gname)

                        # Fallback to Series Genres (names)
                        for gname in series_meta.get("Genres", []) or []:
                            if not gname or gname in added_genre_names:
                                continue
                            key = gname.lower()
                            gid = tv_genre_map.get(key) or movie_genre_map.get(key) or _stable_id_from_name(gname)
                            cur.execute(upsert_genre_sql, (item_id, gid, gname))
                            added_genre_names.add(gname)

                # tags
                added_tag_names = set()
                for t in item.get("TagItems", []) or []:
                    tname = t.get("Name")
                    tid = t.get("Id")
                    if tname:
                        added_tag_names.add(tname)
                    cur.execute(upsert_tag_sql, (item_id, tid, tname))
                for tname in item.get("Tags", []) or []:
                    if not tname or tname in added_tag_names:
                        continue
                    tid = _stable_id_from_name(tname)
                    cur.execute(upsert_tag_sql, (item_id, tid, tname))
                    added_tag_names.add(tname)

                # ProviderIds (TMDB/IMDB/TVDB etc.)
                for provider, pid in (item.get("ProviderIds") or {}).items():
                    if pid:
                        cur.execute(upsert_provider_sql, (item_id, provider, str(pid)))

                batch += 1
                if batch % 1000 == 0:
                    self._connection.commit()
                    self._connection.execute("BEGIN")

            self._connection.commit()
            
            # prune after ingest
            deleted = self.prune_missing_items(seen)
            if self._debug: print(f"Pruned {deleted} items no longer in Emby.")
            
            return True

        except sqlite3.Error as e:
            print(f"[SQLiteConnector] ERROR ingesting library: {e}", file=sys.stderr)
            self._connection.rollback()
            return False
# -------------------------------------------


    
    # ====================================================================== TMDB Tables ======================================================================
    
    def _INIT_create_tmdb_schemas(self):
        """
        Creates (if doesn't exist) the full table schemas for TMDB data.
        
        ---
        Tables: 
        
        `movie_genres`: MOVIE genres as strings, and as the ID values TMDB uses to references genres
        
        `tv_genres`: TV genres as strings, and as the ID values TMDB uses to references genres
        """
        if self._connection is None or self._cursor is None:
            print("[SQLiteConnector] ERROR: DB not connected", file=sys.stderr)
            return False
        
        movie_genres_schema = """
            CREATE TABLE IF NOT EXISTS tmdb_movie_genres(
                id INTEGER PRIMARY KEY UNIQUE NOT NULL,
                name TEXT UNIQUE NOT NULL
            )
        """
        
        tv_genres_schema = """
            CREATE TABLE IF NOT EXISTS tmdb_tv_genres(
                id INTEGER PRIMARY KEY UNIQUE NOT NULL,
                name TEXT UNIQUE NOT NULL
            )
        """
        
        try:
            self._cursor.execute(movie_genres_schema)
            self._cursor.execute(tv_genres_schema)
            
            self._connection.commit()
            
            if self._debug: print("[SQLiteConnector] TMDB schemas created successfully!")
            
        except sqlite3.Error as e:
            if self._debug: print(f"[SQLiteConnector] ERROR: Failed to create TMDB schemas: {e}", file=sys.stderr)
            return False
        
        
    def ingest_tmdb_movie_tv_genres(self, fetch_movie_genre_func: Callable[[], T_TMDBGenres], fetch_tv_genre_func: Callable[[], T_TMDBGenres]):
        """Fetch ALL the genres for BOTH movies and tv shows from TMDB, then ingest them into their appropriate tables"""
        if self._connection is None or self._cursor is None:
            print("[SQLiteConnector] ERROR: DB not connected", file=sys.stderr)
            return False
        
        try:
            for genre in fetch_movie_genre_func():
                self._cursor.execute("INSERT OR REPLACE INTO tmdb_movie_genres(id, name) VALUES(?,?)", (genre["id"], genre["name"]))
            for genre in fetch_tv_genre_func():
                self._cursor.execute("INSERT OR REPLACE INTO tmdb_tv_genres(id, name) VALUES(?,?)", (genre["id"], genre["name"]))
            
            self._connection.commit()
            
            if self._debug: print("Successfully ingested genres into TMDB tables!")
            
        except sqlite3.Error as e:
            if self._debug: print(f"[SQLiteConnector] ERROR: Failed to ingest TMDB genres: {e}", file=sys.stderr)
            return False
