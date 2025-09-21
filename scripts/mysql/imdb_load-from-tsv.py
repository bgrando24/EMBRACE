from typing import Final
from dotenv import load_dotenv
import os
import sys
import mysql.connector
from mysql.connector import Error as MySQLError
import pandas as pd
import numpy as np

# handy thread for issues with "Loading local data is disabled; this must be enabled on both the client and server sides"
# https://stackoverflow.com/questions/59993844/error-loading-local-data-is-disabled-this-must-be-enabled-on-both-the-client

print("\n============================ Running 'imdb_load-from-tsv' Script ============================\n")

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
        database    = DB_NAME,
        allow_local_infile=True,
    )

    if not db.connection_id:
        raise RuntimeError(f"Error with database connection object, current object: {db}")
    
except MySQLError as e:
    print(f"ERROR: Database connection failed: {e}", file=sys.stderr)
    sys.exit(1)

curs: Final = db.cursor()

# load in ALL files - yes this is probably not the most efficient method
# df_principals = pd.read_csv("~/Documents/imdb-db/title.principals.tsv", sep="\t")
# df_principals.replace(r"\N", np.nan, inplace=True)
# df_names = pd.read_csv("~/Documents/imdb-db/name.basics.tsv", sep="\t")
# df_names.replace(r"\N", np.nan, inplace=True)
# df_titles = pd.read_csv("~/Documents/imdb-db/title.basics.tsv", sep="\t")
# df_titles.replace(r"\N", np.nan, inplace=True)
# df_crew = pd.read_csv("~/Documents/imdb-db/title.crew.tsv", sep="\t")
# df_crew.replace(r"\N", np.nan, inplace=True)
# df_episodes = pd.read_csv("~/Documents/imdb-db/title.episode.tsv", sep="\t")
# df_episodes.replace(r"\N", np.nan, inplace=True)
# df_ratings = pd.read_csv("~/Documents/imdb-db/title.ratings.tsv", sep="\t")
# df_ratings.replace(r"\N", np.nan, inplace=True)

# below lists what columns each table actually needs, and from what source
# general process is to extract the required columns from the files, then use them in SQL inserts
# https://dev.mysql.com/doc/refman/8.4/en/optimizing-innodb-bulk-data-loading.html


# Table 'directors': id (auto), t_const (crew), n_const (crew)
# try:
#     # crew_tsv_path = os.path.expanduser("~/Documents/imdb-db/title.crew.tsv")
#     crew_tsv_path = "/import/title.crew.tsv"
#     # if not os.path.isabs(crew_tsv_path):
#     #     crew_tsv_path = os.path.abspath(crew_tsv_path)
#     # if not os.path.exists(crew_tsv_path):
#     #     raise FileNotFoundError(f"Crew TSV not found on server: {crew_tsv_path}")

#     # 1) Staging table mirrors the TSV columns
#     curs.execute("""
#         CREATE TABLE IF NOT EXISTS crew_staging (
#             tconst VARCHAR(12) NOT NULL,
#             directors_csv TEXT NULL,
#             writers_csv   TEXT NULL
#         ) ENGINE=InnoDB
#     """)
#     curs.execute("TRUNCATE TABLE crew_staging")

#     # 2) Bulk load raw TSV into staging (server-side INFILE)
#     crew_load_stage_sql = f"""
#         LOAD DATA INFILE %s
#         INTO TABLE crew_staging
#         FIELDS TERMINATED BY '\\t'
#         LINES TERMINATED BY '\\n'
#         IGNORE 1 LINES
#         (@tconst, @directors_csv, @writers_csv)
#         SET
#           tconst        = NULLIF(@tconst, '\\\\N'),
#           directors_csv = NULLIF(@directors_csv, '\\\\N'),
#           writers_csv   = NULLIF(@writers_csv, '\\\\N')
#     """
#     curs.execute(crew_load_stage_sql, (crew_tsv_path,))

#     # 3) Normalize directors into one row per person
#     curs.execute("""
#         INSERT INTO directors (t_const, n_const)
#         SELECT s.tconst,
#                jt.nconst
#         FROM crew_staging s
#         JOIN JSON_TABLE(
#             CONCAT('["', REPLACE(s.directors_csv, ',', '","'), '"]'),
#             '$[*]' COLUMNS (nconst VARCHAR(12) PATH '$')
#         ) jt
#         WHERE s.directors_csv IS NOT NULL
#               AND s.directors_csv <> ''
#     """)

#     db.commit()

# except (MySQLError, FileNotFoundError) as e:
#     print(f"ERROR: crew table load/normalize failed: {e}", file=sys.stderr)
#     sys.exit(1)

# print("\n~~~~~~~Import for table 'directors' finished!\n")



# Table 'episodes' (episodes): t_const, parent_t_const, season_num, episode_num
try:
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
    curs.execute(episodes_load_stage_sql, (episodes_tsv_path,))

except (MySQLError, FileNotFoundError) as e:
    print(f"ERROR: episodes table data load failed: {e}", file=sys.stderr)
    sys.exit(1)

print("\n~~~~~~~Import for table 'episodes' finished!\n")

# Table 'genres' (title): id (auto), t_const, genre

# Table 'persons' (name): n_const, name, birth_year, death_year

# Table 'ratings' (ratings): t_const, avg_rating, num_votes

# Table 'titles' (title): t_const, type, primary, original, is_adult, start_year, end_year, runtime_minutes

# Table 'writers' (crew): id (auto), t_const, n_const

# Table 'roles' (principals): id (auto), t_const, n_const, category, job, characters (array?)



print("\n============================ Finished 'imdb_load-from-tsv' Script! ============================\n")