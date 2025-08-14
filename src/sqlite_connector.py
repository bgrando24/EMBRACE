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
    cursor: Optional[sqlite3.Cursor]
    _connection: Optional[sqlite3.Connection]

    def __init__(self, DB_NAME: str, debug=False):
        self._debug = debug
        
        if DB_NAME is None or DB_NAME == '':
            print("[SQliteConnector] ERROR: DB_NAME environment variable is not set! Check the .env file at the root of the project.", file=sys.stderr)
        os.makedirs("sqlite_db", exist_ok=True)    # hardcoded on purpose to enforce consistent directories, also helps manage gitignore if DB name is different from default
        self.__DB_DIR: Final = os.path.join("sqlite_db", DB_NAME)
        
    def connect_db(self) -> bool:
        try:
            self._connection = sqlite3.connect(self.__DB_DIR)
            # check db is actually connected and reachable
            self._connection.execute("SELECT 1;")
            if self._debug: print("DB connection tested and successful!")
        except sqlite3.Error as e:
            print(f"[SQliteConnector] ERROR: Database connection failed: {e}", file=sys.stderr)
            self._connection = None
            return False
        # grab cursor, necessary for running queries
        self.cursor = self._connection.cursor()
        return True