# creates the imdb database and loads in the data from TSV files: https://developer.imdb.com/non-commercial-datasets/
# remember to add the file path for where the SQLite database should be stored -> pick somewhere with enough space and fast reads if possible

from typing import Final
from dotenv import load_dotenv
import os
import sys
import mysql.connector
from mysql.connector import Error as MySQLError

print("============================ Running 'imdb_create-and-load' Script ============================")

load_dotenv()
DB_NAME: Final      = os.getenv("MYSQL_DATABASE")
DB_PWD: Final       = os.getenv("MYSQL_ROOT_PASSWORD")
DB_USER: Final      = os.getenv("MYSQL_USER")
DB_USER_PWD: Final  = os.getenv("MYSQL_PASSWORD")
DB_HOST: Final      = os.getenv("MYSQL_HOST")
DB_PORT: Final      = os.getenv("MYSQL_PORT")

# check now if any environment variable is missing, otherwise causes headaches for db connection
required = {
    "MYSQL_DATABASE": DB_NAME,
    "MYSQL_ROOT_PASSWORD": DB_PWD,
    "MYSQL_USER": DB_USER,
    "MYSQL_PASSWORD": DB_USER_PWD,
    "MYSQL_HOST": DB_HOST,
    "MYSQL_PORT": DB_PORT,
}
missing = [k for k, v in required.items() if v in (None, "")]
if missing:
    raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

try:
    # https://dev.mysql.com/doc/connector-python/en/connector-python-connectargs.html
    db = mysql.connector.connect(
        port        = DB_PORT,
        host        = DB_HOST,
        user        = DB_USER,
        password    = DB_USER_PWD,
        # database    = DB_NAME
    )

    if not db.connection_id:
        raise RuntimeError(f"Error with database connection object, current object: {db}")
    
except MySQLError as e:
    print(f"ERROR: Database connection failed: {e}", file=sys.stderr)
    sys.exit(1)

curs: Final = db.cursor()

try:
    curs.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
    curs.execute("USE imdb")
    db.commit()
except MySQLError as e:
    print(f"ERROR: Creating database: {e}", file=sys.stderr)
    sys.exit(1)

try:
    # titles table: titles of a given movie, TV series, or episode
    curs.execute(
        """
        CREATE TABLE IF NOT EXISTS titles (
            t_const VARCHAR(20) PRIMARY KEY NOT NULL,
            type VARCHAR(32),
            primary_name VARCHAR(512),
            original_name VARCHAR(512),
            is_adult TINYINT(1),
            start_year VARCHAR(8),
            end_year VARCHAR(8),
            runtime_minutes INT
        )
        """
    )
    # genres table: stores the one-to-many relationship between a title and its genres
    curs.execute(
        """
            CREATE TABLE IF NOT EXISTS genres (
                id INT AUTO_INCREMENT PRIMARY KEY,
                t_const VARCHAR(20) NOT NULL,
                genre VARCHAR(64) NOT NULL
            )
        """
    )
    # directors table: stores the one-to-many relationship between a title and its directors
    curs.execute(
        """
            CREATE TABLE IF NOT EXISTS directors (
                id INT AUTO_INCREMENT PRIMARY KEY,
                t_const VARCHAR(20) NOT NULL,
                n_const VARCHAR(20) NOT NULL
            )
        """
    )
    # writers table: stores the one-to-many relationship between a title and the featured writers
    curs.execute(
        """
            CREATE TABLE IF NOT EXISTS writers (
                id INT AUTO_INCREMENT PRIMARY KEY,
                t_const VARCHAR(20) NOT NULL,
                n_const VARCHAR(20) NOT NULL
            )
        """
    )
    # episodes table: individual episodes of a TV series
    curs.execute(
        """
            CREATE TABLE IF NOT EXISTS episodes (
                t_const VARCHAR(20) PRIMARY KEY NOT NULL,
                parent_t_const VARCHAR(20) NOT NULL,
                season_num INT,
                episode_num INT
            )
        """
    )
    # roles table: the individual roles of cast/crew for a given title item
    curs.execute(
        """
            CREATE TABLE IF NOT EXISTS roles (
                id INT AUTO_INCREMENT PRIMARY KEY,
                t_const VARCHAR(20) NOT NULL,
                n_const VARCHAR(20) NOT NULL,
                category VARCHAR(64),
                job VARCHAR(500),
                characters TEXT
            )
        """
    )
    # ratings table: self-explanatory
    curs.execute(
        """
            CREATE TABLE IF NOT EXISTS ratings (
                t_const VARCHAR(20) PRIMARY KEY NOT NULL,
                avg_rating DOUBLE,
                num_votes INT
            )
        """
    )
    # persons table: basic details for individual people referenced in other tables 
    curs.execute(
        """
            CREATE TABLE IF NOT EXISTS persons (
                n_const VARCHAR(20) PRIMARY KEY NOT NULL,
                name VARCHAR(256),
                birth_year VARCHAR(8),
                death_year VARCHAR(8)
            )
        """
    )
     
    db.commit()
    
    # double-check tables exist
    curs.execute("SHOW TABLES")
    print(f"Tables in database: \n{curs.fetchall()}")
    db.commit()
    
except MySQLError as e:
    print(f"ERROR: Creating tables failed: {e}", file=sys.stderr)
    sys.exit(1)


print("============================ Script 'db_imdb_create-and-load' Completed! ============================")
