import sqlite3
from sqlite3 import Error
import pandas as pd
import yaml
import sys
import logging


logger = logging.getLogger(__name__)
log_handler = logging.StreamHandler(sys.stdout)
log_handler.setFormatter(
    logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s - %(funcName)s "
        "- line %(lineno)d"
    )
)
logger.addHandler(log_handler)
logger.setLevel(logging.INFO)


def load_config():
    """Returns cert_config.yml file as a python object."""
    with open("cert_config.yml", "r") as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as e:
            logger.critical(e)


def save_config(config):
    """Saves updated `config` object to file as `cert_config.yml` use in conjunction with
    load_config()"""
    with open('cert_config.yml', 'w') as stream:
        yaml.dump(config, stream=stream)


def create_connection(db_name="cert_db.sqlite3"):
    """Creates a connection with specified SQLite3 database in current directory.
    Connection conveniently closes on unindent of with block.

    Typical usage pattern is as follows:
    with create_connection() as conn:
        some_df = pd.read_sql(some_query, conn)
    
    """
    try:
        conn = sqlite3.connect(db_name)
        return conn
    except Error as e:
        logger.critical(e)
    return None


def dbtables_to_csv(db_name="cert_db.sqlite3", destination=""):
    """Writes all tables of specified SQLite3 database to separate CSV files located in
    specified destination subdirectory.
    Not specifying a destination parameter will save CSV files to current directory.

    """
    with create_connection(db_name) as conn:
        table_names = (
            conn.cursor()
            .execute("SELECT name FROM sqlite_master WHERE type='table';")
            .fetchall()
        )
    table_names = [x[0] for x in table_names]
    open_query = "SELECT * FROM {}"
    for table in table_names:
        with create_connection(db_name) as conn:
            pd.read_sql(open_query.format(table), conn).to_csv(
                f"{destination}{'/' if destination else ''}{table}.csv", index=False
            )


if __name__ == "__main__":
    dbtables_to_csv()  # will only ever run with default parameters - no need for argparse
