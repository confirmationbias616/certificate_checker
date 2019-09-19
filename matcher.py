import pandas as pd
import datetime
from wrangler import wrangle
from communicator import communicate
from scorer import compile_score, compile_score_add, attr_score
from matcher_build import match_build
import pickle
from db_tools import create_connection
import re
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


def load_model(version="status_quo"):
    """Unpickles random forest model `rf_model.pkl` from project's root directory and loads
    it into memory.
    
    Parameters:
    version (str): default is `status_quo` but `new` can also be used for validating
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
    version (str): default is `status_quo` but `new` can also be used for validating
    newly-trained models.

    Returns:
    list of strings, each representing a feature (column) of the model (training data).
    """
    logger.debug(f"loading {version} features for learning model")
    version = "" if version == "status_quo" else version + "_"
    with open(f"./{version}rf_features.pkl", "rb") as input_file:
        return pickle.load(input_file)


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
    prob_thresh=0.7,
    multi_phase_proned_thresh=0.97,
    version="status_quo",
):
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
        hist_query = "SELECT * FROM web_certificates WHERE pub_date>=? AND pub_date<=? ORDER BY pub_date"
        with create_connection() as conn:
            df_web = pd.read_sql(hist_query, conn, params=[since, until])
        if (
            len(df_web) == 0
        ):  # SQL query retunred nothing so no point of going any further
            logger.info(
                f"Nothing has been collected from Daily Commercial News since {since}. Breaking out of match function."
            )
            return 0
    df_web = wrangle(df_web)
    comm_count = 0
    for _, company_project_row in company_projects.iterrows():
        results = match_build(
            company_project_row.to_frame().transpose(), df_web
        )  # .iterows returns a pd.Series for every row so this turns it back into a dataframe to avoid breaking any methods downstream
        logger.info(
            f"searching for potential match for project #{company_project_row['job_number']}..."
        )
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
                row.pred_prob, prob_thresh, row.multi_phase_proned, multi_phase_proned_thresh
            ),
            axis=1,
        )
        results = results.sort_values("pred_prob", ascending=False)
        logger.info(
            f"top 5 probabilities: {', '. join([str(round(x, 5)) for x in results.head(5).pred_prob])}"
        )
        matches = results[results.pred_match == 1]
        if len(matches) > 0:
            logger.info(
                f"found {len(matches)} match{'' if len(matches)==1 else 'es'}! with probability as high as {matches.iloc[0].pred_prob}"
            )
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
        f"Done looping through {len(company_projects)} open projects. Sent {comm_count} e-mails to communicate matches as a result."
    )
    return results_master


if __name__ == "__main__":
    match()
