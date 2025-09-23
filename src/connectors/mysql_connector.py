from typing import Final
from dotenv import load_dotenv
import os
import sys
import mysql.connector
from mysql.connector import Error as MySQLError
from pathlib import Path

# helper class to manage connection to, and functionality for, the EMBRACE mysql database
class MySQLConnector:

    tables = [
    "crew_staging", "genres_staging", "directors", "episodes",
    "genres", "persons", "ratings", "titles", "writers", "roles",
    ]

    def __init__(self, env_path: str | os.PathLike[str] | None = None):
        """
        Args:
            env_path (optional): path to a .env file, or a directory containing a .env file.
                - Absolute paths are used as-is.
                - Relative paths are resolved against the project root (parent of 'src').
                - If None, python-dotenv will search from the current working directory upward.
        ---
        ### Example of using project-root relative path:
            ```
            from src.mysql_connector import MySQLConnector
            sql = MySQLConnector(env_path="scripts/mysql/.env")
            ```
        ---
        ### Example of using absolute pathing:
            ```
            from src.mysql_connector import MySQLConnector
            sql = MySQLConnector(env_path="[path to repo]/scripts/mysql/.env")
            ```
        """
        if env_path is None:
            load_dotenv()
        else:
            env_file = Path(env_path).expanduser()
            if not env_file.is_absolute():
                # project root = parent of 'src' (this file lives in src/)
                project_root = Path(__file__).resolve().parents[1]
                # project root = parent of 'src' (this file lives in src/connectors/)
                project_root = Path(__file__).resolve().parents[2]
                env_file = project_root / env_file

            if env_file.is_dir():
                env_file = env_file / ".env"
            if not env_file.exists():
                raise FileNotFoundError(f".env file not found at: {env_file}")

            load_dotenv(dotenv_path=env_file)
        
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
        self._DB_NAME: Final = DB_NAME