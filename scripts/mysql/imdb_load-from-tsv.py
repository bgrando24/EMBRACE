from typing import Final
from dotenv import load_dotenv
import os
import sys
import mysql.connector
from mysql.connector import Error as MySQLError
import pandas as pd
import numpy as np

print("============================ Running 'imdb_load-from-tsv' Script ============================")

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
try:
    directors_tsv_path = os.path.expanduser("~/Documents/imdb-db/title.crew.tsv")
    if not os.path.exists(directors_tsv_path):
        raise FileNotFoundError(f"Directors TSV not found: {directors_tsv_path}")
    # "When doing bulk inserts into tables with auto-increment columns, set innodb_autoinc_lock_mode to 2 (interleaved) instead of 1 (consecutive)."
    # apparently '2' is the default value anyway?
    # curs.execute("SET innodb_autoinc_lock_mode=2")

    # Use MySQL's bulk loader so we stream data straight from the TSV instead of materializing every row in Python.
    load_directors_sql = """
        LOAD DATA LOCAL INFILE %s
        INTO TABLE directors
        FIELDS TERMINATED BY '\\t'
        LINES TERMINATED BY '\\n'
        IGNORE 1 LINES
        (@tConst, @nConst, @writers)
        SET t_const = CASE WHEN @tConst = '' OR @tConst = '\\N' THEN NULL ELSE @tConst END,
            n_const = NULLIF(@nConst, '\\N')
    """

    curs.execute(load_directors_sql, (directors_tsv_path,))
    db.commit()

except (MySQLError, FileNotFoundError) as e:
    print(f"ERROR: Directors load failed: {e}", file=sys.stderr)
    sys.exit(1)

print("\n~~~~~~~Import for table 'directors' finished\n")



# Table 'episodes' (episodes): t_const, parent_t_const, season_num, episode_num

# Table 'genres' (title): id (auto), t_const, genre

# Table 'persons' (name): n_const, name, birth_year, death_year

# Table 'ratings' (ratings): t_const, avg_rating, num_votes

# Table 'titles' (title): t_const, type, primary, original, is_adult, start_year, end_year, runtime_minutes

# Table 'writers' (crew): id (auto), t_const, n_const

# Table 'roles' (principals): id (auto), t_const, n_const, category, job, characters (array?)



print("============================ Finished 'imdb_load-from-tsv' Script! ============================")