import sqlite3

import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)
from utils import create_connection


drop_fts = """
    DROP TABLE cert_search
"""
create_fts = """
    CREATE VIRTUAL TABLE cert_search
    USING FTS3(cert_id, text)
"""
populate_fts = """
    INSERT INTO cert_search
    SELECT cert_id, title || ' ' || owner || ' ' || contractor || ' ' || city as text FROM web_certificates;
"""

def create_or_update_fts():
    try:
        with create_connection() as conn:
            conn.cursor().execute(drop_fts)
    except sqlite3.OperationalError:
        pass
    with create_connection() as conn:
        conn.cursor().execute(create_fts)
        conn.cursor().execute(populate_fts)

if __name__ == "__main__":
    create_or_update_fts()