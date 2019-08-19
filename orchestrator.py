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
    current_datetime = datetime.datetime.now()
    prev_date = current_datetime.day  # just initializing
    run_routine = True  # assume it's a new day on intial run so that daily_routine gets called
    while True:
        if run_routine:
            daily_routine(exit_if_stale=True)
            logger.info(
                "Will be continuously scanning inbox on downtime until something "
                "comes up..."
            )
            run_routine = False  # we ran daily_routine so it's no longer a new day
        try:
            scan_inbox()
            sleep(15)
        except Exception as e: # socket.gaierror:
            logger.info(repr(e))
            logger.info(
                "What the above probably means is that there's no "
                "internet available - retrying in 2 minutes"
            )
            sleep(118)
        current_datetime = datetime.datetime.now()   
        if current_datetime.day == prev_date:  # haven't reached turn of day
            continue
        else:
            prev_date = current_datetime.day
        if current_datetime.isoweekday() in [6,7]:  # (true if Saturday or Sunday
            continue  # nothing posted during weekends
        with create_connection() as conn:
            latest_scrape_date = conn.cursor().execute("""
                SELECT pub_date FROM dcn_certificates 
                ORDER BY pub_date 
                DESC LIMIT 1
            """).fetchone()[0]
        if latest_scrape_date == str(current_datetime.date()):
            continue
        if current_datetime.hour < 4:
            continue
        run_routine = True 
        

if __name__=="__main__":
    orchestrate()
