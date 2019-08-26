import shutil


try:
    shutil.move('./db_backup/2_day/cert_db.sqlite3', './db_backup/3_day/cert_db.sqlite3')
except FileNotFoundError:
    pass
try:
    shutil.move('./db_backup/1_day/cert_db.sqlite3', './db_backup/2_day/cert_db.sqlite3')
except FileNotFoundError:
    pass
try:
    shutil.copy('cert_db.sqlite3', './db_backup/1_day/cert_db.sqlite3')
except FileNotFoundError:
    pass