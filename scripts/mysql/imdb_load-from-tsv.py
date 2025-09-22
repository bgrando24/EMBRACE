# imports data from the TSV files into the database, aligning with table structures

from typing import Final
from dotenv import load_dotenv
import os
import sys
import mysql.connector
from mysql.connector import Error as MySQLError
import time
from contextlib import contextmanager   # docs: https://docs.python.org/3/library/contextlib.html#contextlib.contextmanager

from mysql_connector import MySQLConnector

# helper context manager to standardise performance timing and print statements for table data loads
@contextmanager
def run_table_data_load(tbl_name: str):
    try:
        table_perf_start = time.perf_counter_ns()
        print(f"\n~~~~~~~Starting import for table '{tbl_name}'...\n")
        yield
    except (MySQLError, Exception) as e:
        print(f"ERROR: {tbl_name} table data load failed: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"\n~~~~~~~Import for table '{tbl_name}' finished!\n")

    table_perf_end = time.perf_counter_ns()
    print(
            f"""Data load for table '{tbl_name}' took {table_perf_end - table_perf_start}ns,\n\t
            or {round((table_perf_end - table_perf_start)/1000000000, 2)} seconds"""
        )


print("\n============================ Running 'imsql.db_load-from-tsv' Script ============================\n")

sql = MySQLConnector()
    

# below lists what columns each table actually needs, and from what source
# general process is to extract the required columns from the files, then use them in SQL inserts
# https://dev.mysql.com/doc/refman/8.4/en/optimizing-innosql.db-bulk-data-loading.html


# ------------------ Table 'directors': id (auto), t_const (crew), n_const (crew)
with run_table_data_load("directors"):
    
    crew_tsv_path = "/import/title.crew.tsv"

    # 1) Staging table mirrors the TSV columns
    sql.curs.execute("""
        CREATE TABLE IF NOT EXISTS crew_staging (
            tconst VARCHAR(12) NOT NULL,
            directors_csv TEXT NULL,
            writers_csv   TEXT NULL
        ) ENGINE=InnoDB
    """)
    sql.curs.execute("TRUNCATE TABLE crew_staging")

    # 2) Bulk load raw TSV into staging (server-side INFILE)
    crew_load_stage_sql = f"""
        LOAD DATA INFILE %s
        INTO TABLE crew_staging
        FIELDS TERMINATED BY '\\t'
        LINES TERMINATED BY '\\n'
        IGNORE 1 LINES
        (@tconst, @directors_csv, @writers_csv)
        SET
          tconst        = NULLIF(@tconst, '\\\\N'),
          directors_csv = NULLIF(@directors_csv, '\\\\N'),
          writers_csv   = NULLIF(@writers_csv, '\\\\N')
    """
    sql.curs.execute(crew_load_stage_sql, (crew_tsv_path,))

    # 3) Normalize directors into one row per person
    sql.curs.execute("""
        INSERT INTO directors (t_const, n_const)
        SELECT s.tconst,
               jt.nconst
        FROM crew_staging s
        JOIN JSON_TABLE(
            CONCAT('["', REPLACE(s.directors_csv, ',', '","'), '"]'),
            '$[*]' COLUMNS (nconst VARCHAR(12) PATH '$')
        ) jt
        WHERE s.directors_csv IS NOT NULL
              AND s.directors_csv <> ''
    """)

    sql.curs.execute("SELECT COUNT(*) FROM directors")
    print(f"Rows in 'directors' table after data load: {sql.curs.fetchall()}")
    sql.db.commit()



# ------------------ Table 'episodes' (episodes): t_const, parent_t_const, season_num, episode_num
with run_table_data_load("episodes"):
    episodes_tsv_path = "/import/title.episode.tsv"

    episodes_load_stage_sql = f"""
            LOAD DATA INFILE %s
            INTO TABLE episodes
            FIELDS TERMINATED BY '\\t'
            LINES TERMINATED BY '\\n'
            IGNORE 1 LINES
            (@tconst, @parentTconst, @seasonNumber, @episodeNumber)
            SET
            t_const         = NULLIF(@tconst, '\\\\N'),
            parent_t_const  = NULLIF(@parentTconst, '\\\\N'),
            season_num      = NULLIF(@seasonNumber, '\\\\N'),
            episode_num     = NULLIF(@episodeNumber, '\\\\N')
        """
    sql.curs.execute(episodes_load_stage_sql, (episodes_tsv_path,))
    
    sql.curs.execute("SELECT COUNT(*) FROM episodes")
    print(f"Rows in 'episodes' table after data load: {sql.curs.fetchall()}")
    
    sql.db.commit()


# ------------------ Table 'genres' (title): id (auto), t_const, genre
with run_table_data_load("genres"):
    basics_tsv_path = "/import/title.basics.tsv"

    # 1) staging table mirrors the TSV columns
    sql.curs.execute("""
        CREATE TABLE IF NOT EXISTS genres_staging (
            tconst VARCHAR(12) NOT NULL,
            genres TEXT NULL
        ) ENGINE=InnoDB
    """)
    sql.curs.execute("TRUNCATE TABLE genres_staging")

    # 2) Bulk load raw TSV into staging (server-side INFILE)
    genres_load_stage_sql = f"""
        LOAD DATA INFILE %s
        INTO TABLE genres_staging
        FIELDS TERMINATED BY '\\t'
        LINES TERMINATED BY '\\n'
        IGNORE 1 LINES
        (@tconst, @titleType, @primaryTitle, @originalTitle, @isAdult, @startYear, @endYear, @runtimeMinutes, @genres)
        SET
          tconst        = NULLIF(@tconst, '\\\\N'),
          genres        = NULLIF(@genres, '\\\\N')
    """
    sql.curs.execute(genres_load_stage_sql, (basics_tsv_path,))

    # 3) normalise genres into one row per title genre tag
    sql.curs.execute("""
        INSERT INTO genres (t_const, genre)
        SELECT s.tconst,
               jt.genre
        FROM genres_staging s
        JOIN JSON_TABLE(
            CONCAT('["', REPLACE(s.genres, ',', '","'), '"]'),
            '$[*]' COLUMNS (genre VARCHAR(64) PATH '$')
        ) jt
        WHERE s.genres IS NOT NULL
              AND s.genres <> ''
    """)
    
    sql.curs.execute("SELECT COUNT(*) FROM genres")
    print(f"Rows in 'genres' table after data load: {sql.curs.fetchall()}")
    
    sql.db.commit()



# ------------------ Table 'persons' (name): n_const, name, birth_year, death_year
with run_table_data_load("persons"):
    persons_tsv_path = "/import/name.basics.tsv"

    persons_load_stage_sql = f"""
            LOAD DATA INFILE %s
            INTO TABLE persons
            FIELDS TERMINATED BY '\\t'
            LINES TERMINATED BY '\\n'
            IGNORE 1 LINES
            (@nconst, @primaryName, @birthYear, @deathYear, @primaryProfession, @knownForTitles)
            SET
            n_const         = NULLIF(@nconst, '\\\\N'),
            name            = NULLIF(@primaryName, '\\\\N'),
            birth_year      = NULLIF(@birthYear, '\\\\N'),
            death_year      = NULLIF(@deathYear, '\\\\N')
        """
    sql.curs.execute(persons_load_stage_sql, (persons_tsv_path,))
    
    sql.curs.execute("SELECT COUNT(*) FROM persons")
    print(f"Rows in 'persons' table after data load: {sql.curs.fetchall()}")
    
    sql.db.commit()



# ------------------ Table 'ratings' (ratings): t_const, avg_rating, num_votes
with run_table_data_load("ratings"):
    ratings_tsv_path = "/import/title.ratings.tsv"

    ratings_load_stage_sql = f"""
            LOAD DATA INFILE %s
            INTO TABLE ratings
            FIELDS TERMINATED BY '\\t'
            LINES TERMINATED BY '\\n'
            IGNORE 1 LINES
            (@tconst, @averageRating, @numVotes)
            SET
            t_const         = NULLIF(@tconst, '\\\\N'),
            avg_rating      = NULLIF(@averageRating, '\\\\N'),
            num_votes       = NULLIF(@numVotes, '\\\\N')
        """
    sql.curs.execute(ratings_load_stage_sql, (ratings_tsv_path,))
    
    sql.curs.execute("SELECT COUNT(*) FROM ratings")
    print(f"Rows in 'ratings' table after data load: {sql.curs.fetchall()}")
    
    sql.db.commit()



# ------------------ Table 'titles' (title): t_const, type, primary, original, is_adult, start_year, end_year, runtime_minutes
with run_table_data_load("titles"):
    basics_tsv_path = "/import/title.basics.tsv"

    titles_load_sql = f"""
            LOAD DATA INFILE %s
            INTO TABLE titles
            FIELDS TERMINATED BY '\\t'
            LINES TERMINATED BY '\\n'
            IGNORE 1 LINES
            (@tconst, @titleType, @primaryTitle, @originalTitle, @isAdult, @startYear, @endYear, @runtimeMinutes, @genres)
            SET
            t_const         = NULLIF(@tconst, '\\\\N'),
            type            = NULLIF(@titleType, '\\\\N'),
            primary_name    = NULLIF(@primaryTitle, '\\\\N'),
            original_name   = NULLIF(@originalTitle, '\\\\N'),
            is_adult        = NULLIF(@isAdult, '\\\\N'),
            start_year      = NULLIF(@startYear, '\\\\N'),
            end_year        = NULLIF(@endYear, '\\\\N'),
            runtime_minutes = NULLIF(@runtimeMinutes, '\\\\N')
        """
    sql.curs.execute(titles_load_sql, (basics_tsv_path,))

    sql.curs.execute("SELECT COUNT(*) FROM titles")
    print(f"Rows in 'titles' table after data load: {sql.curs.fetchall()}")

    sql.db.commit()

# ------------------ Table 'writers' (crew): id (auto), t_const, n_const
with run_table_data_load("writers"):
    # Reuse crew_staging already populated in the 'directors' step
    sql.curs.execute("""
        INSERT INTO writers (t_const, n_const)
        SELECT s.tconst,
               jt.nconst
        FROM crew_staging s
        JOIN JSON_TABLE(
            CONCAT('["', REPLACE(s.writers_csv, ',', '","'), '"]'),
            '$[*]' COLUMNS (nconst VARCHAR(12) PATH '$')
        ) jt
        WHERE s.writers_csv IS NOT NULL
              AND s.writers_csv <> ''
    """)

    sql.curs.execute("SELECT COUNT(*) FROM writers")
    print(f"Rows in 'writers' table after data load: {sql.curs.fetchall()}")

    sql.db.commit()

# ------------------ Table 'roles' (principals): id (auto), t_const, n_const, category, job
with run_table_data_load("roles"):
    principals_tsv_path = "/import/title.principals.tsv"

    roles_load_sql = f"""
            LOAD DATA INFILE %s
            INTO TABLE roles
            FIELDS TERMINATED BY '\\t'
            LINES TERMINATED BY '\\n'
            IGNORE 1 LINES
            (@tconst, @ordering, @nconst, @category, @job, @characters)
            SET
            t_const     = NULLIF(@tconst, '\\\\N'),
            n_const     = NULLIF(@nconst, '\\\\N'),
            category    = NULLIF(@category, '\\\\N'),
            job         = NULLIF(@job, '\\\\N'),
            characters  = NULLIF(@characters, '\\\\N')
        """
    sql.curs.execute(roles_load_sql, (principals_tsv_path,))

    sql.curs.execute("SELECT COUNT(*) FROM roles")
    print(f"Rows in 'roles' table after data load: {sql.curs.fetchall()}")

    sql.db.commit()



print("\n============================ Finished 'imsql.db_load-from-tsv' Script! ============================\n")
