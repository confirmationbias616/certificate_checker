import shutil
import datetime
import dateutil
from os import listdir
import re


try:
    today_date = datetime.datetime.now()
    week_old_date = dateutil.parser.parse(re.findall('\d{4}-\d{2}-\d{2}', listdir('./db_backup/week_old')[0])[0])
    if (today_date - week_old_date).days > 7:
        shutil.move('./db_backup/3_day/cert_db.sqlite3', f'./db_backup/week_old/cert_db-{today_date.date()}.sqlite3')
        print('copied to week_old')
    else:
        print('not copied to week_old yet because not stale enough')
except Exception as e:
    print(f'could not copy to week_old due to :\n{e}')
try:
    shutil.move('./db_backup/2_day/cert_db.sqlite3', './db_backup/3_day/cert_db.sqlite3')
    print('copied to day_3')
except FileNotFoundError:
    print('could not copy to day_3')
try:
    shutil.move('./db_backup/1_day/cert_db.sqlite3', './db_backup/2_day/cert_db.sqlite3')
    print('copied to day_2')
except FileNotFoundError:
    print('could not copy to day_2')
try:
    shutil.copy('cert_db.sqlite3', './db_backup/1_day/cert_db.sqlite3')
    print('copied to day_1')
except FileNotFoundError:
    print('could not copy to day_1')