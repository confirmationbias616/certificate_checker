import os
from log_user_input import log_user_input
from scraper import scrape
from matcher import match
from ml import build_train_set, train_model
import sys
import logging

prob_thresh = 0.65

def daily_routine():
    logger.info('initiating daily routine...')
    log_user_input()
    scrape()
    build_train_set()
    train_model(prob_thresh=prob_thresh)
    match(since='2019-05-07', prob_thresh=prob_thresh)  #test=True to mute sending of e-mails

if __name__=="__main__":
    logger = logging.getLogger(__name__)
    log_handler = logging.StreamHandler(sys.stdout)
    log_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s - %(funcName)s - line %(lineno)d"
        )
    )
    logger.addHandler(log_handler)
    logger.setLevel(logging.INFO)
    try:
        os.rename('temp_cert_db', 'cert_db')
    except:
        pass
    daily_routine()