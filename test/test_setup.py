import os
import pandas as pd
from utils import create_connection


def create_test_db():
    abs_dir_path = os.path.abspath(__file__).replace("test/test_setup.py", "")
    os.chdir(abs_dir_path + "test/")
    csv_file_names = [x for x in os.listdir() if x.endswith(".csv")]
    for csv_table_name in csv_file_names:
        with create_connection(db_name="test_cert_db.sqlite3") as conn:
            pd.read_csv(csv_table_name).to_sql(csv_table_name[:-4], conn, index=False)
    os.rename(
        abs_dir_path + "test/test_cert_db.sqlite3",
        abs_dir_path + "test_cert_db.sqlite3",
    )
    os.chdir(abs_dir_path)
