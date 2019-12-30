import add_parent_to_path
import shutil
import datetime
import dateutil.parser
from os import listdir
import re


for filename, extension in [['cert_db', '.sqlite3'], ['cache_geocode_city', '.json']]:
    try:
        today_date = datetime.datetime.now()
        week_old_date = dateutil.parser.parse(
            re.findall("\d{4}-\d{2}-\d{2}", listdir("./db_backup/week_old")[0])[0]
        )
        if (today_date - week_old_date).days > 7:
            shutil.move(
                f"./db_backup/3_day/{filename}{extension}",
                f"./db_backup/week_old/{filename}_{today_date.date()}{extension}",
            )
            print(f"copied {filename} to week_old")
        else:
            print(f"did not copy {filename} to week_old yet because not stale enough")
    except Exception as e:
        print(f"could not copy {filename} to week_old due to :\n{e}")
    try:
        shutil.move(
            f"./db_backup/2_day/{filename}{extension}", f"./db_backup/3_day/{filename}{extension}"
        )
        print(f"copied {filename} to day_3")
    except FileNotFoundError:
        print(f"could not copy {filename} to day_3")
    try:
        shutil.move(
            f"./db_backup/1_day/{filename}{extension}", f"./db_backup/2_day/{filename}{extension}"
        )
        print(f"copied {filename} to day_2")
    except FileNotFoundError:
        print(f"could not copy {filename} to day_2")
    try:
        shutil.copy(f"{filename}{extension}", f"./db_backup/1_day/{filename}{extension}")
        print(f"copied to {filename} day_1")
    except FileNotFoundError:
        print(f"could not copy {filename} to day_1")
