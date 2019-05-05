import sqlite3
from sqlite3 import Error


def create_connection(db_name='cert_db'):
    try:
        conn = sqlite3.connect(db_name)
        return conn
    except Error as e:
        print(e)
    return None