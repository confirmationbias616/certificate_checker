import os
import shutil
import pandas as pd
from utils import create_connection


def create_test_db():
    abs_dir_path = os.path.abspath(__file__).replace("test/test_setup.py", "")
    os.chdir(abs_dir_path + "test/")
    csv_file_names = [x for x in os.listdir() if x.endswith(".csv")]
    for csv_table_name in csv_file_names:
        with create_connection(db_name="test_cert_db.sqlite3") as conn:
            pd.read_csv(csv_table_name).to_sql(csv_table_name[:-4], conn, index=False)
    # Execute migration scripts below to modify on newly populated tables
    with create_connection(db_name="test_cert_db.sqlite3") as conn:
        conn.cursor().executescript("""
            PRAGMA foreign_keys=off;
            ALTER TABLE company_projects RENAME TO old_table;
            CREATE TABLE company_projects (
                project_id INTEGER PRIMARY KEY,
                job_number TEXT,
                city TEXT,
                address TEXT,
                title TEXT,
                contractor TEXT,
                owner TEXT,
                engineer TEXT,
                closed INTEGER,
                receiver_emails_dump TEXT,
                address_lat REAL,
                address_lng REAL, 
                city_lat REAL,
                city_lng REAL,
                city_size REAL,
                company_id TEXT NOT NULL,
                last_cert_id_check INTEGER
            );
            INSERT INTO company_projects SELECT * FROM old_table;
            DROP TABLE old_table;
            PRAGMA foreign_keys=on;
        """)
        conn.cursor().executescript("""
            PRAGMA foreign_keys=off;
            ALTER TABLE web_certificates RENAME TO old_table;
            CREATE TABLE web_certificates (
                cert_id INT PRIMARY KEY NOT NULL,
                pub_date TEXT,
                city TEXT,
                address TEXT,
                title TEXT,
                owner TEXT,
                contractor TEXT,
                engineer TEXT,
                url_key TEXT,
                source VARCHAR DEFAULT "dcn",
                cert_type VARCHAR DEFAULT "csp",
                address_lat REAL,
                address_lng REAL,
                city_lat REAL,
                city_lng REAL,
                city_size REAL
            );
            INSERT INTO web_certificates SELECT * FROM old_table;
            DROP TABLE old_table;
            PRAGMA foreign_keys=on;
        """)
        conn.cursor().executescript("""
            PRAGMA foreign_keys=off;
            ALTER TABLE contacts RENAME TO old_table;
            CREATE TABLE contacts (
                id INTEGER PRIMARY KEY,
                company_id TEXT NOT NULL,
                name TEXT,
                email_address TEXT
            );
            INSERT INTO contacts SELECT * FROM old_table;
            DROP TABLE old_table;
            PRAGMA foreign_keys=on;
        """)
    os.rename(
        abs_dir_path + "test/test_cert_db.sqlite3",
        abs_dir_path + "test_cert_db.sqlite3",
    )
    shutil.copy(
        abs_dir_path + "test/results.json",
        abs_dir_path + "results.json",
    )
    os.chdir(abs_dir_path)
