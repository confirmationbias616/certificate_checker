import os
from inbox_scanner import scan_inbox
from scraper import scrape
from matcher import match
from ml import build_train_set, train_model, validate_model
import sys
import logging

prob_thresh = 0.7

def daily_routine():
    logger.info('initiating daily routine...')
    logger.info('scan inbox once to process new e-mails')
    scan_inbox(continuously=False)
    logger.info('scrape')
    scrape()
    logger.info('build_train_set')
    build_train_set()
    logger.info('train_model')
    train_model(prob_thresh=prob_thresh)
    logger.info('match')
    match(prob_thresh=prob_thresh)  #test=True to mute sending of e-mails
    logger.info('validate')
    validate_model(prob_thresh=prob_thresh)

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
    for filename in ['cert_db.sqlite3', 'rf_model.pkl', 'rf_features.pkl']:
        try:
            os.rename('temp_'+filename, filename)
        except:
            pass
    daily_routine()
