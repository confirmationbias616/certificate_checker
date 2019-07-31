import datetime
import logging
import sys
from time import sleep
from inbox_scanner import scan_inbox
from daily_script import daily_routine
from db_tools import create_connection


logger = logging.getLogger(__name__)
log_handler = logging.StreamHandler(sys.stdout)
log_handler.setFormatter(
    logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s - %(funcName)s - line %(lineno)d"
    )
)
logger.addHandler(log_handler)
logger.setLevel(logging.INFO)

def orchestrate():
    logger.info("Starting up orchestration.")
    logger.info("Will be continuously scanning inbox on downtime until something comes up...")
    new_day = True  # assume it's a new day on intial run
    while True:
        try:
            scan_inbox()
        except:  # socket.gaierror:
            logger.info('no internet available - retrying...')
        current_datetime = datetime.datetime.now()
        hist_query = "SELECT pub_date FROM df_hist ORDER BY pub_date DESC LIMIT 1"
        with create_connection() as conn:
            latest_scrape_date = conn.cursor().execute(hist_query).fetchone()[0]
        try:  # will raise NameError if initial run
            if current_datetime.hour <= prev_datetime.hour:
                new_day = True  # while loop already ran today
        except NameError:
            new_day = True
        if (
            (current_datetime.hour >= 4) and
            new_day and
            current_datetime.isoweekday() <= 5  and # True if weekday
            latest_scrape_date != str(current_datetime.date())
        ):
            logger.info(
                f"Since it's a passed 4 AM on a weekday for which data hasn't "
                f"been scraped yet, launching daily_routine.")
            daily_routine()
            new_day = False
        prev_datetime = current_datetime
        sleep(2)
        

if __name__=="__main__":
    orchestrate()
