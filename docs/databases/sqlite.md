# SQLite Database Documentation

### User watch history schemas

Data relevant to a user's watch history.

#### `watch_hist_raw_events` table

-   `row_id` - UUID for each event (auto-incremented)
-   `date` - Date of event, **NOTE: UTC+10, Australia/Melbourne timezone**
-   `time` - Time of event, **NOTE: UTC+10, Australia/Melbourne timezone**
-   `user_id` - Emby's ID for the user
-   `item_name` - Name of item
-   `item_id` - Emby's ID for the item
-   `item_type` - Type of the item (Movie, Episode)
-   `duration` - Duration of the watch event (in seconds)
-   `remote_address` - IP address of the user's device (optional)
-   `user_name` - User's Emby username

#### `watch_hist_agg_sessions` table

-   `session_id` - Unique identifier for each session (auto-incremented)
-   `user_id` - Emby's ID for the user
-   `item_id` - Emby's ID for the item
-   `item_name` - Name of watched item
-   `item_type` - Type of the item (Movie, Episode)
-   `session_start_timestamp` - First event timestamp
-   `session_end_timestamp` - Last event timestamp
-   `session_duration_minutes` - session_duration_minutes = session_end_timestamp - session_start_timestamp
-   `total_seconds_watched` - Total seconds watched in the session
-   `session_count` - Number of raw events in 'this' session
-   `completion_ratio` - completion_ratio = total_seconds_watched / actual_runtime_seconds
-   `outcome` - Outcome or status of the session
    -   **Outcome Classification:**
        -   **sampled**: less than 30 seconds total watched
        -   **abandoned**: less than 20% completion and more than 5 minutes watched
        -   **partial**: 20–80% completion
        -   **completed**: 80% or greater completion
-   `created_timestamp` - Timestamp when the session record was created (defaults to current time)
-   `UNIQUE(user_id, item_id, DATE(session_start_timestamp))` - Ensures uniqueness for user, item, and date combinations

#### `watch_hist_user_item_stats` table

-   `stat_id` - Unique identifier for each user-item stat record (auto-incremented)
-   `user_id` - Emby's ID for the user
-   `item_id` - Emby's ID for the item
-   `item_name` - Name of watched item
-   `item_type` - Type of the item (Movie, Episode)
-   `total_sessions` - Total number of viewing sessions for this user-item pair
-   `total_seconds_watched` - Cumulative seconds watched across all sessions
-   `total_minutes_watched` - Auto-calculated field (total_seconds_watched / 60.0)
-   `best_completion_ratio` - Highest completion percentage achieved in any single session
-   `average_completion_ratio` - Average completion percentage across all sessions
-   `rewatch_count` - Number of sessions beyond the first (total_sessions - 1)
-   `first_watched_timestamp` - Timestamp of the user's first viewing session
-   `last_watched_timestamp` - Timestamp of the user's most recent viewing session
-   `days_between_first_last` - Auto-calculated days between first and last watch (0 if same day)
-   `adherence_score` - Computed preference strength score (0.0 - 1.0 range)
-   `completed_sessions` - Count of sessions with 'completed' outcome
-   `partial_sessions` - Count of sessions with 'partial' outcome
-   `abandoned_sessions` - Count of sessions with 'abandoned' outcome
-   `sampled_sessions` - Count of sessions with 'sampled' outcome
-   `last_updated_timestamp` - Timestamp when this record was last modified (defaults to current time)
-   `UNIQUE(user_id, item_id)` - Ensures one record per user-item combination

**Key Relationships:**

-   Aggregates data from multiple records in `watch_hist_agg_sessions`
-   One record per unique user-item pair across all time
-   Updated whenever new sessions are processed for the user-item combination

### SQLite Indexes

#### `idx_watch_hist_raw_user_time`

**Table:** `watch_hist_raw_events`  
**Columns:** `(user_id, date, time DESC)`  
**Purpose:** Optimizes queries for user-specific watch history ordered by time  
**Common Use Cases:**

-   Fetching recent watch events for a user
-   Building user timelines
-   Incremental data processing (get events since last sync)

#### `idx_watch_hist_agg_sessions`

**Table:** `watch_hist_agg_sessions`  
**Columns:** `(user_id, session_end_timestamp DESC)`  
**Purpose:** Speeds up user session queries sorted by recency  
**Common Use Cases:**

-   "Continue watching" functionality
-   Recent activity feeds
-   User session analysis and patterns

#### `idx_watch_hist_user_item_stats`

**Table:** `watch_hist_user_item_stats`  
**Columns:** `(user_id, adherence_score DESC)`  
**Purpose:** Optimizes queries for user's top-rated/most-watched content  
**Common Use Cases:**

-   Building user taste profiles
-   Finding user's favorite content
-   Recommendation engine input (top adherence items)
-   User preference analysis

**Note:** All indexes include `IF NOT EXISTS` to prevent errors during schema recreation.

---

</br></br>
