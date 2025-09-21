from typing import Final
from dotenv import load_dotenv
import os
import sys
import mysql.connector
from mysql.connector import Error as MySQLError

print("============================ Running 'imdb_creset' Script ============================")

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
    curs.execute("USE imdb")
    # curs.execute(f"DROP DATABASE imdb")
    curs.execute("TRUNCATE TABLE directors")
    curs.execute("TRUNCATE TABLE episodes")
    curs.execute("TRUNCATE TABLE genres")
    curs.execute("TRUNCATE TABLE persons")
    curs.execute("TRUNCATE TABLE ratings")
    curs.execute("TRUNCATE TABLE titles")
    curs.execute("TRUNCATE TABLE writers")
    curs.execute("TRUNCATE TABLE roles")
    
    db.commit()
except MySQLError as e:
    print(f"ERROR: Unable to reset database: {e}", file=sys.stderr)
    sys.exit(1)

print("\n============================ Finished 'imdb_reset' Script! ============================\n")