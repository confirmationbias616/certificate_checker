import add_parent_to_path
import os
from matcher import match
from ml import build_train_set, train_model, validate_model
from utils import load_config, load_results
import datetime
from dateutil.parser import parse as parse_date
import sys
import logging

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


def daily_routine():
    logger.info("initiating daily routine...")
    prob_thresh = load_config()["machine_learning"]["prboability_thresholds"]["general"]
    if "train" in load_config()["daily_routine"]["steps"]:
        try:
            logger.info("build_train_set")
            build_train_set()
            logger.info("train_model")
            train_model(prob_thresh=prob_thresh, use_smote=False)
        except Exception as e:
            logger.critical(e)
    if "validate" in load_config()["daily_routine"]["steps"]:
        try:
            logger.info("validate")
            validate_model(prob_thresh=prob_thresh)
        except Exception as e:
            logger.critical(e)
    if "match" in load_config()["daily_routine"]["steps"]:
        try:
            logger.info("match")
            last_run = sorted(list(load_results().keys()))[-1]
            if (datetime.datetime.now() - parse_date(last_run)).days < 2:
                since = str((datetime.datetime.now()-datetime.timedelta(2)).date())
            else:
                since = last_run
            match(since=since, prob_thresh=prob_thresh, test=load_config()["daily_routine"]["test"])
        except Exception as e:
            logger.critical(e)


if __name__ == "__main__":
    for filename in ["cert_db.sqlite3", "rf_model.pkl", "rf_features.pkl", "results.json"]:
        try:
            os.rename("temp_" + filename, filename)
        except:
            pass
    daily_routine()
