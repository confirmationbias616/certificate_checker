import sqlite3
from sqlite3 import Error


def create_connection():
    try:
        conn = sqlite3.connect('cert_db')
        return conn
    except Error as e:
        print(e)
    return None