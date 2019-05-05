import sqlite3
from sqlite3 import Error
import pandas as pd


def create_connection(db_name='cert_db'):
    try:
        conn = sqlite3.connect(db_name)
        return conn
    except Error as e:
        print(e)
    return None

def dbtables_to_csv():
    with create_connection() as conn:
        table_names = conn.cursor().execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
    table_names = [x[0] for x in table_names]

    open_query = "SELECT * FROM {}"
    for table in table_names:
        with create_connection() as conn:
            pd.read_sql(open_query.format(table), conn).drop('index', axis=1).to_csv('{}.csv'.format(table), index=False)