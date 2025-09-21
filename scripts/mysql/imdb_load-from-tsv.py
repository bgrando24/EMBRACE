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
        database    = DB_NAME
    )

    if not db.connection_id:
        raise RuntimeError(f"Error with database connection object, current object: {db}")
    
except MySQLError as e:
    print(f"ERROR: Database connection failed: {e}", file=sys.stderr)
    sys.exit(1)

curs: Final = db.cursor()

# load in ALL files - yes this is probably not the most efficient method
df_principals = pd.read_csv("~/Documents/imdb-db/title.principals.tsv", sep="\t")
df_principals.replace(r"\N", np.nan, inplace=True)
df_names = pd.read_csv("~/Documents/imdb-db/name.basics.tsv", sep="\t")
df_names.replace(r"\N", np.nan, inplace=True)
df_titles = pd.read_csv("~/Documents/imdb-db/title.basics.tsv", sep="\t")
df_titles.replace(r"\N", np.nan, inplace=True)
df_crew = pd.read_csv("~/Documents/imdb-db/title.crew.tsv", sep="\t")
df_crew.replace(r"\N", np.nan, inplace=True)
df_episodes = pd.read_csv("~/Documents/imdb-db/title.episode.tsv", sep="\t")
df_episodes.replace(r"\N", np.nan, inplace=True)
df_ratings = pd.read_csv("~/Documents/imdb-db/title.ratings.tsv", sep="\t")
df_ratings.replace(r"\N", np.nan, inplace=True)

# below lists what columns each table actually needs, and from what source
# general process is to extract the required columns from the files, then use them in SQL inserts


# Table 'directors': id (auto), t_const (crew), n_const (crew)
directors_table = df_crew[["tconst", "directors"]].copy().to_numpy()
for row in directors_table:
    curs.execute(f"INSERT INTO directors (t_const, n_const) VALUES {row[0]}, {row[1]}")


# Table 'episodes' (episodes): t_const, parent_t_const, season_num, episode_num

# Table 'genres' (title): id (auto), t_const, genre

# Table 'persons' (name): n_const, name, birth_year, death_year

# Table 'ratings' (ratings): t_const, avg_rating, num_votes

# Table 'roles' (principals): id (auto), t_const, n_const, category, job, characters (array?)

# Table 'titles' (title): t_const, type, primary, original, is_adult, start_year, end_year, runtime_minutes

# Table 'writers' (crew): id (auto), t_const, n_const