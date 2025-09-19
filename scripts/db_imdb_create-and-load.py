# creates the imdb database and loads in the data from TSV files: https://developer.imdb.com/non-commercial-datasets/
# remember to add the file path for where the SQLite database should be stored -> pick somewhere with enough space and fast reads if possible

from typing import Final
from dotenv import load_dotenv
import os
import sqlite3
import sys

print("============================ Running 'db_imdb_create-and-load' Script ============================")

load_dotenv()

IMDB_DB_PATH: Final = os.getenv("IMDB_DB_PATH")
if (IMDB_DB_PATH == None):
    print("ERROR: 'IMDB_DB_PATH' environment variable is not set! Check your .env", file=sys.stderr)
    exit(1)

DB_NAME: Final = "imdb.db"
DB_DIR: Final = os.path.join(IMDB_DB_PATH, DB_NAME)

conn: sqlite3.Connection
curs: sqlite3.Cursor

# create directory, then connect to db (creates db file if not exists), then create tables
os.makedirs(IMDB_DB_PATH, exist_ok=True)
        
try:
    conn = sqlite3.connect(DB_DIR)
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
                id INTEGER PRIMARY KEY NOT NULL AUTOINCREMENT,
                t_const TEXT FOREIGN KEY NOT NULL,
                genre TEXT NOT NULL
            )
        """
    )
    # directors table: stores the one-to-many relationship between a title and its directors
    curs.execute(
        """
            CREATE TABLE IF NOT EXISTS directors (
                id INTEGER PRIMARY KEY NOT NULL AUTOINCREMENT,
                t_const TEXT FOREIGN KEY NOT NULL,
                director TEXT NOT NULL
            )
        """
    )
    # title_writers_map table: stores the one-to-many relationship between a title and the featured writers
    curs.execute(
        """
            CREATE TABLE IF NOT EXISTS title_writers_map (
                id INTEGER PRIMARY KEY NOT NULL AUTOINCREMENT,
                t_const TEXT FOREIGN KEY NOT NULL,
                writer TEXT NOT NULL
            )
        """
    )
    # episodes table: individual episodes of a TV series
    curs.execute(
        """
            CREATE TABLE IF NOT EXISTS episodes (
                t_const TEXT PRIMARY KEY NOT NULL,
                parent_t_const TEXT FOREIGN KEY NOT NULL,
                season_num INTEGER,
                episode_num INTEGER
            )
        """
    )
    # roles table: the individual roles of cast/crew for a given title item
    curs.execute(
        """
            CREATE TABLE IF NOT EXISTS roles (
                id INTEGER PRIMARY KEY NOT NULL AUTOINCREMENT,
                t_const TEXT FOREIGN KEY NOT NULL,
                n_const TEXT FOREIGN KEY NOT NULL,
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