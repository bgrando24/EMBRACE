# creates the imdb database and loads in the data from TSV files: https://developer.imdb.com/non-commercial-datasets/
# remember to add the file path for where the SQLite database should be stored -> pick somewhere with enough space and fast reads if possible

from typing import Final
from dotenv import load_dotenv
import os
import sqlite3
import sys
from pathlib import Path

print("============================ Running 'db_imdb_create-and-load' Script ============================")

load_dotenv()

# remember to set database path in .env file
RAW_DB_PATH: Final = os.getenv("IMDB_DB_PATH")
if not RAW_DB_PATH:
    print("ERROR: 'IMDB_DB_PATH' environment variable is not set!", file=sys.stderr)
    sys.exit(1)

# expanduser helps manage "~" whether linux or macos
db_dir = Path(RAW_DB_PATH).expanduser()
if not db_dir.parts:
    print("ERROR: 'IMDB_DB_PATH' resolves to an empty path.", file=sys.stderr)
    sys.exit(1)

try:
    db_dir.mkdir(parents=True, exist_ok=True)
except OSError as exc:
    print(f"ERROR: Unable to create '{db_dir}': {exc}", file=sys.stderr)
    sys.exit(1)

DB_NAME: Final = "imdb.db"
DB_PATH: Final = db_dir / DB_NAME
        
conn: sqlite3.Connection
curs: sqlite3.Cursor

try:
    conn = sqlite3.connect(DB_PATH)
    # check db is actually connected and reachable
    conn.execute("SELECT 1;")
    print("DB connection tested and successful!")
except sqlite3.Error as e:
    print(f"ERROR: Database connection failed: {e}", file=sys.stderr)
    exit(1)
    
curs = conn.cursor()


# name.basics.tsv  title.basics.tsv  title.episode.tsv  title.ratings.tsv
# title.akas.tsv   title.crew.tsv    title.principals.tsv

try:
    # titles table: titles of a given movie, TV series, or episode
    curs.execute(
        """
        CREATE TABLE IF NOT EXISTS titles (
            t_const TEXT PRIMARY KEY NOT NULL,
            title_type TEXT,
            primary_title TEXT,
            original_title TEXT,
            is_adult INTEGER,
            start_year TEXT,
            end_year TEXT,
            runtime_minutes REAL
        )
        """
    )
    # genres table: stores the one-to-many relationship between a title and its genres
    curs.execute(
        """
            CREATE TABLE IF NOT EXISTS genres (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                t_const TEXT NOT NULL,
                genre TEXT NOT NULL
            )
        """
    )
    # directors table: stores the one-to-many relationship between a title and its directors
    curs.execute(
        """
            CREATE TABLE IF NOT EXISTS directors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                t_const TEXT NOT NULL,
                director TEXT NOT NULL
            )
        """
    )
    # title_writers_map table: stores the one-to-many relationship between a title and the featured writers
    curs.execute(
        """
            CREATE TABLE IF NOT EXISTS title_writers_map (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                t_const TEXT NOT NULL,
                writer TEXT NOT NULL
            )
        """
    )
    # episodes table: individual episodes of a TV series
    curs.execute(
        """
            CREATE TABLE IF NOT EXISTS episodes (
                t_const TEXT PRIMARY KEY NOT NULL,
                parent_t_const TEXT NOT NULL,
                season_num INTEGER,
                episode_num INTEGER
            )
        """
    )
    # roles table: the individual roles of cast/crew for a given title item
    curs.execute(
        """
            CREATE TABLE IF NOT EXISTS roles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                t_const TEXT NOT NULL,
                n_const TEXT NOT NULL,
                category TEXT,
                job TEXT,
                characters TEXT
            )
        """
    )
    # ratings table: self-explanatory
    curs.execute(
        """
            CREATE TABLE IF NOT EXISTS ratings (
                t_const TEXT PRIMARY KEY NOT NULL,
                avg_rating REAL,
                num_votes INTEGER
            )
        """
    )
    # persons table: basic details for individual people referenced in other tables 
    curs.execute(
        """
            CREATE TABLE IF NOT EXISTS persons (
                n_const TEXT PRIMARY KEY NOT NULL,
                name TEXT,
                birth_year TEXT,
                death_year TEXT
            )
        """
    )
     
    conn.commit()
    
except sqlite3.Error as e:
    print(f"ERROR: Creating tables failed: {e}", file=sys.stderr)
    exit(1)


print("============================ Script 'db_imdb_create-and-load' Completed! ============================")