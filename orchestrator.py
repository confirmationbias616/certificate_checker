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
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s - %(funcName)s "
        "- line %(lineno)d"
    )
)
logger.addHandler(log_handler)
logger.setLevel(logging.INFO)

def orchestrate():
    logger.info("Starting up orchestration.")
    logger.info(
        "Will be continuously scanning inbox on downtime until something "
        "comes up..."
    )
    new_day = True  # assume it's a new day on intial run
    daily_routine(exit_if_stale=True)  # run daily routine once intially
    while True:
        if not new_day:
            try:
                scan_inbox()
            except Exception as e: # socket.gaierror:
                logger.info(repr(e))
                logger.info(
                    "What the above probably means is that there's no "
                    "internet available - retrying in 2 minutes"
                )
                sleep(118)
        # Check if new_day flag shold be renewed as `True`
        hist_query = """
            SELECT pub_date FROM dcn_certificates 
            ORDER BY pub_date 
            DESC LIMIT 1
            """
        with create_connection() as conn:
            latest_scrape_date = conn.cursor().execute(hist_query).fetchone()[0]
        try:  # will raise NameError if initial run
            prev_datetime = current_datetime
        except NameError:
            pass
        current_datetime = datetime.datetime.now()
        try:  # will raise NameError if initial run
            if current_datetime.hour < prev_datetime.hour:
                new_day = True  # while loop already ran today
        except NameError:
            pass
        if current_datetime.isoweekday() in [1,7]:  # (true if Monday or Sunday)
            new_day = False  # known days where there's 0 new certs @4AM
            logger.info("going back to listening for incoming e-mails...")
        if (
            (current_datetime.hour >= 4) and
            new_day and
            (latest_scrape_date not in [
                str(current_datetime.date()),
                str(current_datetime.date()-datetime.timedelta(1))
                ])
            ):
            logger.info(
                "Since it's a passed 4 AM on a weekday for which data hasn't "
                "been scraped yet, launching daily_routine.")
            daily_routine(exit_if_stale=True)
            logger.info("going back to listening for incoming e-mails...")
        new_day = False
        

if __name__=="__main__":
    orchestrate()
