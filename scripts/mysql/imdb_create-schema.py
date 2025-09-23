# creates the imdb database and loads in the data from TSV files: https://developer.imdb.com/non-commercial-datasets/
# remember to add the file path for where the SQLite database should be stored -> pick somewhere with enough space and fast reads if possible
import sys
from mysql.connector import Error as MySQLError
# add project root (parent of 'scripts') to sys.path so 'src' is importable
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from connectors import MySQLConnector

print("============================ Running 'imdb_create-and-load' Script ============================")

sql = MySQLConnector("scripts/mysql/.env")

try:
    sql.curs.execute(f"CREATE DATABASE IF NOT EXISTS {sql._DB_NAME}")
    sql.curs.execute("USE imdb")
    sql.db.commit()
except MySQLError as e:
    print(f"ERROR: Creating database: {e}", file=sys.stderr)
    sys.exit(1)

try:
    # titles table: titles of a given movie, TV series, or episode
    sql.curs.execute(
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
    sql.curs.execute(
        """
            CREATE TABLE IF NOT EXISTS genres (
                id INT AUTO_INCREMENT PRIMARY KEY,
                t_const VARCHAR(20) NOT NULL,
                genre VARCHAR(64) NOT NULL
            )
        """
    )
    # directors table: stores the one-to-many relationship between a title and its directors
    sql.curs.execute(
        """
            CREATE TABLE IF NOT EXISTS directors (
                id INT AUTO_INCREMENT PRIMARY KEY,
                t_const VARCHAR(20) NOT NULL,
                n_const VARCHAR(20) NOT NULL
            )
        """
    )
    # writers table: stores the one-to-many relationship between a title and the featured writers
    sql.curs.execute(
        """
            CREATE TABLE IF NOT EXISTS writers (
                id INT AUTO_INCREMENT PRIMARY KEY,
                t_const VARCHAR(20) NOT NULL,
                n_const VARCHAR(20) NOT NULL
            )
        """
    )
    # episodes table: individual episodes of a TV series
    sql.curs.execute(
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
    sql.curs.execute(
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
    sql.curs.execute(
        """
            CREATE TABLE IF NOT EXISTS ratings (
                t_const VARCHAR(20) PRIMARY KEY NOT NULL,
                avg_rating DOUBLE,
                num_votes INT
            )
        """
    )
    # persons table: basic details for individual people referenced in other tables 
    sql.curs.execute(
        """
            CREATE TABLE IF NOT EXISTS persons (
                n_const VARCHAR(20) PRIMARY KEY NOT NULL,
                name VARCHAR(256),
                birth_year VARCHAR(8),
                death_year VARCHAR(8)
            )
        """
    )
     
    sql.db.commit()
    
    # double-check tables exist
    sql.curs.execute("SHOW TABLES")
    print(f"Tables in database: \n{sql.curs.fetchall()}")
    sql.db.commit()
    
except MySQLError as e:
    print(f"ERROR: Creating tables failed: {e}", file=sys.stderr)
    sys.exit(1)


print("============================ Script 'db_imdb_create-and-load' Completed! ============================")
