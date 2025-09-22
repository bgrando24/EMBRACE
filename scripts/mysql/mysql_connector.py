from typing import Final
from dotenv import load_dotenv
import os
import sys
import mysql.connector
from mysql.connector import Error as MySQLError

# helper class to manage connection to, and functionality for, the EMBRACE mysql database
class MySQLConnector:

    def __init__(self):
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
            self.db = mysql.connector.connect(
                port        = DB_PORT,
                host        = DB_HOST,
                user        = DB_USER,
                password    = DB_USER_PWD,
                database    = DB_NAME,
                allow_local_infile=True,
            )

            if not self.db.connection_id:
                raise RuntimeError(f"Error with database connection object, current object: {self.db}")
            
        except MySQLError as e:
            print(f"ERROR: Database connection failed: {e}", file=sys.stderr)
            sys.exit(1)

        self.curs: Final = self.db.cursor()