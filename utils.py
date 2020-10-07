import sqlite3
from sqlite3 import Error
import pandas as pd
import yaml
import sys
import logging
import json
import datetime
import argparse


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
    """Returns `cert_config.yml` file as a python object."""
    with open("cert_config.yml", "r") as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as e:
            logger.critical(e)


def load_results():
    """Returns `results.json` file as a python object."""
    with open('results.json') as f:
        return json.load(f)


def load_last_recorded(key):
    """Returns last known recorded value of input `key`."""
    results = load_results()
    last_recorded_date = sorted(list(results.keys()))[-1]
    return results[last_recorded_date][key]


def update_results(new_results):
    """Updates `results.json` by updating dictionary located at key for today's date with whatever
    dictionary is passed in."""
    today_date = str(datetime.datetime.now().date())
    results = load_results()
    if results.get(today_date, False):
        results[today_date].update(new_results)
    else:
        results.update({today_date : new_results})
    with open('results.json', 'w') as f:
        json.dump(results, f, sort_keys=True, indent=2)  


def save_config(config):
    """Saves updated `config` object to file as `cert_config.yml` use in conjunction with
    load_config()"""
    with open("cert_config.yml", "w") as stream:
        yaml.dump(config, stream=stream)


def persistant_cache(file_name):
    """Creates and/or updates persitant cache in json format with given filename."""
    def decorator(original_func):
        try:
            cache = json.load(open(file_name, 'r'))
        except (IOError, ValueError):
            cache = {}
        def new_func(param):
            if param not in cache:
                cache[param] = original_func(param)
                json.dump(cache, open(file_name, 'w'), indent=4)
            return cache[param]
        return new_func
    return decorator


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

def custom_query(query):
    """Run custom SQL query against cert_db.sqlite3"""
    try:
        with create_connection() as conn:
            result =  pd.read_sql(query, conn)
        print(result)
        return result
    except TypeError:
        with create_connection() as conn:
            conn.cursor().execute(query)
        print("SQL statement executed.")
        return

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
    table_names = [x[0] for x in table_names if 'cert_search' not in x[0]]
    open_query = "SELECT * FROM {}"
    for table in table_names:
        with create_connection(db_name) as conn:
            pd.read_sql(open_query.format(table), conn).to_csv(
                f"{destination}{'/' if destination else ''}{table}.csv", index=False
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Call specific utility fucntion.')
    parser.add_argument('--custom_query', type=str, help="""
        Run custom SQL query against cert_db.sqlite3
    """)
    parser.add_argument('--update_test_csv_files',  action='store_true', help="""
        Writes all tables of specified SQLite3 database to separate CSV files located in
        `./test` subdirectory.
    """)
    args = parser.parse_args()
    if args.custom_query:
        custom_query(args.custom_query)
    elif args.update_test_csv_files:
        dbtables_to_csv(destination="./test")
    else:
        parser.error('No action requested, add argument according to --help')
