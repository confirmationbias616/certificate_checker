import sqlite3

import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)
from utils import create_connection


drop_fts = """
    DROP TABLE cert_search
"""
clean_string_column = """
    UPDATE 
	    web_certificates
    SET
        {} = ' '
    WHERE
        {} IS NULL
"""
create_fts = """
    CREATE VIRTUAL TABLE cert_search
    USING FTS3(cert_id, text)
"""
populate_fts = """
    INSERT INTO cert_search
    SELECT 
        cert_id, 
        title || ' ' || owner || ' ' || contractor || ' ' || city || ' ' || engineer as text 
    FROM web_certificates;
"""

def update_fts():
    with create_connection() as conn:
        try:
            conn.cursor().execute(drop_fts)  # drop fts table if existing so it can be rewritten
            conn.commit()
        except sqlite3.OperationalError:
            pass
        for column in ['title', 'owner', 'contractor', 'city', 'engineer']:
            conn.cursor().execute(clean_string_column.format(column, column))
            conn.commit()
        conn.cursor().execute(create_fts)
        conn.commit()
        conn.cursor().execute(populate_fts)
        conn.commit()

if __name__ == "__main__":
    update_fts()