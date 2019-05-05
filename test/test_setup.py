import os
from db_tools import create_connection


csv_file_names = [x for x in os.listdir() if x.endswith(".csv")]
for csv_table_name in csv_file_names:
    with create_connection(db_name='test_cert_db') as conn:
        pd.read_csv(csv_table_name).to_sql(csv_table_name[:-4], conn, index=False)

abs_dir_path = os.path.abspath(__file__).replace('test/test_setup.py','')
os.rename(abs_dir_path+"test/test_cert_db", abs_dir_path+"cert_db")