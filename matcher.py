import pandas as pd
import datetime
from wrangler import wrangle
from communicator import communicate
from scorer import build_match_score
import pickle
from utils import create_connection, load_config, update_results
import re
import sys
import logging
import argparse
import mysql.connector


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


def load_model(version="status_quo"):
    """Unpickles random forest model `rf_model.pkl` from project's root directory and loads
    it into memory.
    
    Parameters:
    `version` (str): default is `status_quo` but `new` can also be used for validating
    newly-trained models.

    Returns:
    a trained instance of scikit-learn's RandomForestClassifier, which has been previously
    trained and saved in project's root directory.
    
    """
    logger.debug(f"loading {version} random forest classifier")
    version = "" if version == "status_quo" else version + "_"
    with open(f"./{version}rf_model.pkl", "rb") as input_file:
        return pickle.load(input_file)


def load_feature_list(version="status_quo"):
    """Unpickles list of features used in matching version of `rf_model.pkl` from project's
    root directory and loads it into memory.
    
    Parameters:
    `version` (str): default is `status_quo` but `new` can also be used for validating
    newly-trained models.

    Returns:
    list of strings, each representing a feature (column) of the model (training data).
    
    """
    logger.debug(f"loading {version} features for learning model")
    version = "" if version == "status_quo" else version + "_"
    with open(f"./{version}rf_features.pkl", "rb") as input_file:
        return pickle.load(input_file)


def predict_prob(sample, version="status_quo"):
    """Predicts probability of match (entity resolution) between company project and web
    certificate.
    
    Parameters:
     - `sample` (pd.Series or str): dataframe or filename of csv with root folder of a
     table containing single row of pre-wranggled, pre-scored, and pre-built proposed match.
     - `version` (str): default is `status_quo` but `new` can also be used for validating
     newly-trained models.

    Returns:
     - probsbility of match (float value ranging from 0 to 1). Note that this does not take
     into consideration the probability threshold, which is meant to deviate from 0.5.
    
    """
    if not isinstance(sample, pd.Series):
        try:
            pd.read_csv(sample).iloc[0]
        except:
            raise TypeError(
                "Need to pass in a Pandas Series or filename of csv file within root"
                " folder, whcih contsains a single row of data (after header)."
            )
    clf = load_model(version=version)
    cols = load_feature_list(version=version)
    prob = clf.predict_proba(sample[cols].values.reshape(1, -1))[0][1]
    return prob


def predict_match(prob, prob_thresh, multi_phase_proned, multi_phase_proned_thresh):
    """Predicts whether or not there's a match (entity resolution) for a given prediction
    probability.
    
    Parameters:
     - `prob` (float): prediction probability provided by trained random forest classifier.
     - `prob_thresh` (float): probability threshold for decision boundary.
     - `multi_phase_proned` (int): (1 or 0) whether or not a project is identified as being
     at risk of having multiple phases, which means it's more likely to be a false positive.
     - `multi_phase_proned_thresh` (float): probability threshold for projects which are
     identified as being at risk of having multiple phases, which will override the standard
     prob_thresh. This value should be set higher than prob_thresh.
    
    Returns:
     - 1 or 0 (match or not)
    
    """
    if multi_phase_proned:
        prob_thresh = multi_phase_proned_thresh
    if prob >= prob_thresh:
        return 1
    else:
        return 0


def match(
    company_projects=False,
    df_web=False,
    test=False,
    since="today",
    until="now",
    prob_thresh=load_config()["machine_learning"]["prboability_thresholds"]["general"],
    multi_phase_proned_thresh=load_config()["machine_learning"][
        "prboability_thresholds"
    ]["multi_phase"],
    version="status_quo",
):
    """Combines company projects and web CSP certificates in all-to-all join, wrangles the
    rows, scores the rows as potential matches, runs each row through Random Forest model,
    and communicates results via log and, if deemed successful, email as well.

    TODO: THIS FUNCTION IS TOO LONG AND DOES WAY TOO MANY THINGS. MUST BE REFACTORED ASAP.
    
    Parameters:
     - `company_projects` (pd.DataFrame): specify dataframe of company projects to match
     instead of default, which is to retreive all open projects from `company_projects` table
     in databse.
     - `df_web` (pd.DataFrame): specify dataframe of CSP certificates to match instead
     of default, which is to retreive all open projects from `web_certificates` table
     in databse according to specified timeframe.
     - `test` (bool): whether in testing or not, will dtermine flow of operations and mute
     emails appropriately.
     - `since` (str of format `"yyyy-mm-dd"`): used in conjunction with `until` to specify
     timeframe to query database for `df_web`. Only used if `df_web` not specified. Special
     strings `"week_ago"`, `"day_ago"`, or `"today"` can be used instead. Range is inclusive
     of date specified.
     - `until` (str of format `"yyyy-mm-dd"`): used in conjunction with `since` to specify
     timeframe to query database for `df_web`. Only used if `df_web` not specified. Special
     string `"now"` can be used instead. Range is inclusive
     of date specified.
     - `prob_thresh` (float): probability threshold for decision boundary.
     - `multi_phase_proned_thresh` (float): probability threshold for projects which are
     identified as being at risk of having multiple phases, which will override the standard
     prob_thresh. This value should be set higher than prob_thresh.
     - `version` (str): default is `status_quo` but `new` can also be used for validating
     newly-trained models.

    Returns:
     - a Pandas DataFrame containing all of certificate info, project number it was attempted
     to be matched with, and score results. Length of dataframe should be the length of
     `company_projects` x `df_web`. Mostly used for testing purposes.
     - `False` if there were no CSP certificates available for timeframe specified through
     `since` and `until`.

    """
    logger.info("matching...")
    if not isinstance(company_projects, pd.DataFrame):  # company_projects == False
        open_query = "SELECT * FROM company_projects WHERE closed=0"
        with create_connection() as conn:
            company_projects = pd.read_sql(open_query, conn)
    company_projects = wrangle(company_projects)
    if not isinstance(df_web, pd.DataFrame):  # df_web == False
        if since == "today":
            since = datetime.datetime.now().date()
        elif since == "day_ago":
            since = (datetime.datetime.now() - datetime.timedelta(1)).date()
        elif since == "week_ago":
            since = (datetime.datetime.now() - datetime.timedelta(7)).date()
        else:
            try:
                since = re.findall("\d{4}-\d{2}-\d{2}", since)[0]
            except KeyError:
                raise ValueError(
                    "`since` parameter should be in the format yyyy-mm-dd if not a key_word"
                )
        if until == "now":
            until = datetime.datetime.now()
        else:
            try:
                until = re.findall("\d{4}-\d{2}-\d{2}", until)[0]
            except KeyError:
                raise ValueError(
                    "`until` parameter should be in the format yyyy-mm-dd if not a key_word"
                )
        hist_query = """
            SELECT * 
            FROM web_certificates
            WHERE pub_date>=%s AND pub_date<=%s
            ORDER BY pub_date
        """
        with create_connection() as conn:
            df_web = pd.read_sql(hist_query, conn, params=[since, until])
        if (
            len(df_web) == 0
        ):  # SQL query retunred nothing so no point of going any further
            logger.info(
                f"No new CSP's have been collected since last time `match()` was called ({since}). "
                f"Breaking out of match function."
            )
            update_results({
                'match summary': 'nothing new to match', 
                'noteworthy matches' : {}
            })
            return False
    df_web = wrangle(df_web)
    comm_count = 0
    for _, company_project_row in company_projects.iterrows():
        results = build_match_score(
            company_project_row.to_frame().transpose(), df_web, fresh_cert_limit=(not test)
        )  # .iterows returns a pd.Series for every row so this turns it back into a dataframe to avoid breaking any methods downstream
        logger.info(
            f"searching for potential match for project #{company_project_row['job_number']}..."
        )
        results["job_number"] = company_project_row.job_number
        results["multi_phase_proned"] = results.apply(
            lambda row: 1
            if any(
                re.findall(
                    "campus|hospital|university|college",
                    "".join(row[["city", "title"]].apply(str)),
                )
            )
            else 0,
            axis=1,
        )
        results["pred_prob"] = results.apply(
            lambda row: predict_prob(row, version=version), axis=1
        )
        results["pred_match"] = results.apply(
            lambda row: predict_match(
                row.pred_prob,
                prob_thresh,
                row.multi_phase_proned,
                multi_phase_proned_thresh,
            ),
            axis=1,
        )
        results = results.sort_values("pred_prob", ascending=False)
        logger.info(
            f"top 5 probabilities for project #{company_project_row['job_number']}: "
            f"{', '. join([str(round(x, 5)) for x in results.head(5).pred_prob])}"
        )
        matches = results[results.pred_match == 1]
        if len(matches) > 0:
            logger.info(
                f"found {len(matches)} match{'' if len(matches)==1 else 'es'}! with "
                f"probability as high as {matches.iloc[0].pred_prob}"
            )
            if not test:
                logger.info("getting ready to send notification...")
                communicate(
                    matches.drop(matches.index[1:]),  # sending only top result for now
                    company_project_row,
                    test=test,
                )
                comm_count += 1
        else:
            logger.info("didn't find any matches")
        try:
            results_master = results_master.append(results)
        except NameError:
            results_master = results
    logger.info(
        f"Done looping through {len(company_projects)} open projects. Sent {comm_count} "
        f"e-mails to communicate matches as a result."
    )
    update_results({
        'match summary': f"matched {comm_count} out of {len(company_projects)} projects and {int(len(results_master)/len(company_projects))} CSP's",
        'noteworthy matches' : results_master[results_master.pred_prob > 0.5][['cert_id','job_number', 'pred_prob', 'pred_match']].to_dict()
    })
    return results_master


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--since", type=str, help="date from when to begin looking for matches"
    )
    parser.add_argument(
        "--until", type=str, help="date for when to stop search for matches"
    )
    args = parser.parse_args()
    kwargs = {}
    if args.since:
        kwargs["since"] = args.since
    if args.since:
        kwargs["until"] = args.until
    match(**kwargs)
