import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from imblearn.over_sampling import SMOTE
from sklearn.model_selection import KFold
from sklearn.metrics import f1_score
import pickle
from wrangler import wrangle
from matcher import match
from scorer import build_match_score
from utils import create_connection, load_config, update_results
import sys
import logging
import os
import datetime


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


def save_model(model):
    """Pickles current machine learning model nd saves it to the project's root directory."""
    logger.debug("saving random forest classifier")
    with open("./new_rf_model.pkl", "wb") as output:
        pickle.dump(model, output)


def save_feature_list(columns):
    """Pickles a list of features used in current machine learning model and saves it to
    the project's root directory."""
    logger.debug("saving list of features for random forest classifier")
    with open("./new_rf_features.pkl", "wb") as output:
        pickle.dump(columns, output)


def build_train_set():
    """Builds training dataset by extracting relevant rows from `web_certificates` and 
    `company_projects` tables within cert_db databse, wrangling the data, and combining
    it in various ways. Saves to project root directory as Pandas Dataframe."""
    logger.info("building dataset for training random forest classifier")
    match_query = """
        SELECT
            company_projects.job_number,
            company_projects.city,
            company_projects.address,
            company_projects.title,
            company_projects.owner,
            company_projects.contractor,
            company_projects.engineer,
            company_projects.address_lat,
            company_projects.address_lng,
            web_certificates.url_key,
            company_projects.receiver_emails_dump
        FROM 
            web_certificates
        LEFT JOIN
            attempted_matches
        ON
            web_certificates.cert_id = attempted_matches.cert_id
        LEFT JOIN
            company_projects
        ON
            attempted_matches.project_id = company_projects.project_id
        LEFT JOIN
            base_urls
        ON
            base_urls.source = web_certificates.source
        WHERE 
            company_projects.closed=1
        AND
            attempted_matches.ground_truth=1
        AND 
            attempted_matches.multi_phase=0
        AND 
            attempted_matches.validate=0
    """
    corr_web_certs_query = """
        SELECT
            web_certificates.*
        FROM 
            web_certificates
        LEFT JOIN
            attempted_matches
        ON
            web_certificates.cert_id = attempted_matches.cert_id
        LEFT JOIN
            company_projects
        ON
            attempted_matches.project_id = company_projects.project_id
        LEFT JOIN
            base_urls
        ON
            base_urls.source = web_certificates.source
        WHERE 
            company_projects.closed=1
        AND
            attempted_matches.ground_truth=1
        AND 
            attempted_matches.multi_phase=0
        AND 
            attempted_matches.validate=0
    """

    with create_connection() as conn:
        test_company_projects = pd.read_sql(match_query, conn)
        test_web_df = pd.read_sql(corr_web_certs_query, conn)
    test_web_df = wrangle(test_web_df)

    # Get some certificates that are definitely not matches to provide some false matches to train from
    start_date = "2011-01-01"
    end_date = "2011-04-30"
    hist_query = "SELECT * FROM web_certificates WHERE pub_date BETWEEN ? AND ? ORDER BY pub_date"
    with create_connection() as conn:
        rand_web_df = pd.read_sql(hist_query, conn, params=[start_date, end_date])
    rand_web_df = wrangle(rand_web_df)

    for i, test_company_row in test_company_projects.iterrows():
        test_company_row = wrangle(
            test_company_row.to_frame().transpose()
        )  # .iterows returns a pd.Series for every row so this turns it back into a dataframe to avoid breaking any methods downstream
        rand_web_df = rand_web_df.sample(n=len(test_company_projects), random_state=i)
        close_matches = build_match_score(test_company_row, test_web_df, fresh_cert_limit=False)
        random_matches = build_match_score(test_company_row, rand_web_df, fresh_cert_limit=False)
        matches = close_matches.append(random_matches)
        matches["ground_truth"] = matches.url_key.apply(
            lambda x: 1 if x == test_company_row.url_key.iloc[0] else 0
        )
        matches["title_length"] = matches.title.apply(len)
        try:
            all_matches = all_matches.append(matches)
        except NameError:
            all_matches = matches
    all_matches.to_csv("./train_set.csv", index=False)


def train_model(
    prob_thresh=load_config()["machine_learning"]["prboability_thresholds"]["general"],
    smote_data=False
):
    """Trains instance of scikit-learn's RandomForestClassifier model on the training dataset
    from project's root directory (typically produced by function ml.build_train_set) and saves
    trained model to root directory as well.
    
    Parameters:
     - `prob_thresh` (float): probability threshold which the classifier will use to determine
     whether or not there is a match. Scikit-learn's default threshold is 0.5 but this is being
     disregarded. Note that this threshold doesn't impact the actual training of the model - 
     only its custom predictions and performance metrics.
     - `smote_data` (boolean): whether or not the SMOTE algorithm should be applied to the labeled
     data before training the model. Default False.

    Returns:
     - rc_cum (float): average recall
     - pr_cum (float): average precision
     - f1_cum (float): avergae f1 score
    """
    logger.info("training random forest classifier")
    df = pd.read_csv("./train_set.csv")
    exclude_fetures = load_config()["machine_learning"]["exclude_features"]
    X = df[[x for x in df.columns if x.endswith("_score") and x not in exclude_fetures]]
    save_feature_list(X.columns)
    feature_list = list(X.columns)
    update_results({'features' : feature_list})
    y = df[["ground_truth"]]
    clf = RandomForestClassifier(n_estimators=100, max_depth=5, random_state=42)
    sm = SMOTE(random_state=42, ratio=1)
    kf = KFold(n_splits=3, shuffle=True, random_state=41)
    rc_cum, pr_cum, f1_cum = [], [], []
    split_no = 0
    for train_index, test_index in kf.split(X):
        split_no += 1
        logger.info(f"K-Split #{split_no}...")
        X_train, X_test = X.values[train_index], X.values[test_index]
        y_train, y_test = y.values[train_index], y.values[test_index]
        if smote_data:
            X_train_final, y_train_final = sm.fit_sample(X_train, y_train)
        else:
            X_train_final, y_train_final = X_train, y_train
        clf.fit(X_train_final, y_train_final)
        prob = clf.predict_proba(X_test)
        pred = [1 if x >= prob_thresh else 0 for x in clf.predict_proba(X_test)[:, 1]]
        y_test = y_test.reshape(
            y_test.shape[0]
        )  # shitty little workaround required due to pandas -> numpy  conversion
        results = pd.DataFrame(
            {
                "truth": y_test,
                "total_score": X_test[:, -1],
                "prob": prob[:, 1],
                "pred": pred,
            }
        )
        rc = len(results[(results.truth == 1) & (results.pred == 1)]) / len(
            results[results.truth == 1]
        )
        pr = len(results[(results.truth == 1) & (results.pred == 1)]) / len(
            results[results.pred == 1]
        )
        f1 = f1_score(y_test, pred)
        logger.info(
            f"number of truthes to learn from: {len([x for x in y_train if x==1])} out of {len(y_train)}"
        )
        logger.info(f"number of tests: {len(results[results.truth==1])}")
        feat_imp = pd.DataFrame(
            {"feat": X.columns, "imp": clf.feature_importances_}
        ).sort_values("imp", ascending=False)
        logger.debug("\nfeat_imp\n")
        logger.info("top features are:")
        for _, row in feat_imp.iterrows():
            logger.info(
                f"`{row['feat']}` -> {round(row['imp'], 3)}"
            )
        logger.info(f"recall: {round(rc, 3)}")
        logger.info(f"precision: {round(pr, 3)}")
        logger.info(f"f1 score: {round(f1, 3)}")
        rc_cum.append(rc)
        pr_cum.append(pr)
        f1_cum.append(f1)
    logger.info(f"average recall: {round(sum(rc_cum)/len(rc_cum), 3)}")
    logger.info(f"average precision: {round(sum(pr_cum)/len(pr_cum), 3)}")
    logger.info(f"avergae f1 score: {round(sum(f1_cum)/len(f1_cum), 3)}")
    if smote_data:
        X_final, y_final = sm.fit_sample(X, y)
    else:
        X_final, y_final = X, y 
    clf.fit(X_final, y_final)
    save_model(clf)
    return rc_cum, pr_cum, f1_cum


def validate_model(**kwargs):
    """Compares new model with status quo production model and compiles/reports the results.
    Based on results, will either replace model and archive old one or just maintain status quo.
    
    Parameters:
     - `prob_thresh` (float): probability threshold which the classifier will use to determine
     whether or not there is a match.
     - `test` (bool): whether in testing or not, will dtermine flow of operations and mute emails appropriately.

    """
    try:
        test = kwargs["test"]
    except KeyError:
        test = False
    match_query = """
        SELECT
            company_projects.job_number,
            company_projects.city,
            company_projects.address,
            company_projects.title,
            company_projects.owner,
            company_projects.contractor,
            company_projects.engineer,
            company_projects.address_lat,
            company_projects.address_lng,
            company_projects.receiver_emails_dump,
            web_certificates.url_key,
            attempted_matches.ground_truth,
            attempted_matches.multi_phase,
            web_certificates.pub_date,
            web_certificates.source,
            (base_urls.base_url || web_certificates.url_key) AS link
        FROM
            web_certificates
        LEFT JOIN
            attempted_matches
        ON
            web_certificates.cert_id = attempted_matches.cert_id
        LEFT JOIN
            company_projects
        ON
            attempted_matches.project_id = company_projects.project_id
        LEFT JOIN
            base_urls
        ON
            base_urls.source = web_certificates.source
        WHERE 
            company_projects.closed=1
        AND
            attempted_matches.ground_truth=1
        AND 
            attempted_matches.multi_phase=0
        AND 
            attempted_matches.validate=1
    """
    corr_web_certs_query = """
        SELECT
            web_certificates.*
        FROM 
            web_certificates
        LEFT JOIN
            attempted_matches
        ON
            web_certificates.cert_id = attempted_matches.cert_id
        LEFT JOIN
            company_projects
        ON
            attempted_matches.project_id = company_projects.project_id
        LEFT JOIN
            base_urls
        ON
            base_urls.source = web_certificates.source
        WHERE 
            company_projects.closed=1
        AND
            attempted_matches.ground_truth=1
        AND 
            attempted_matches.multi_phase=0
        AND 
            attempted_matches.validate=1
    """
    with create_connection() as conn:
        validate_company_projects = pd.read_sql(match_query, conn)
        validate_web_df = pd.read_sql(corr_web_certs_query, conn)
    new_results = match(
        version="new",
        company_projects=validate_company_projects,
        df_web=validate_web_df,
        test=True,
        prob_thresh=kwargs["prob_thresh"],
    )
    # check if 100% recall for new model
    qty_actual_matches = int(len(new_results) ** 0.5)
    qty_found_matches = new_results[new_results.pred_match == 1].title.nunique()
    full_recall = qty_found_matches == qty_actual_matches
    # the below exception will happen if there was no existing model present in
    # folder (in testing) important to not skip validation so that the function
    # can be propperly tested
    try:
        sq_results = match(
            version="status_quo",
            company_projects=validate_company_projects,
            df_web=validate_web_df,
            test=True,
            prob_thresh=kwargs["prob_thresh"],
        )
    except FileNotFoundError:
        logger.info(
            "could not find any status quo models to use for baseline validation."
        )
        if not test:
            logger.info("adopting new model by default and skipping rest of validation")
            for filename in ["rf_model.pkl", "rf_features.pkl"]:
                os.rename("new_" + filename, filename)
            return  # exit function because there is no basline to validate against
        else:
            logger.info(
                "will keep testing validation using new model as baseline. Just for testing purposes."
            )
            sq_results = match(
                version="new",
                company_projects=validate_company_projects,
                df_web=validate_web_df,
                test=True,
                prob_thresh=kwargs["prob_thresh"],
            )
    # check out how many false positives were generated with status quo model and new model
    sq_false_positives = len(sq_results[sq_results.pred_match == 1]) - qty_found_matches
    new_false_positives = (
        len(new_results[new_results.pred_match == 1]) - qty_found_matches
    )
    # pull out some stats
    sq_pred_probs = sq_results[sq_results.pred_match == 1]
    new_pred_probs = new_results[new_results.pred_match == 1]
    sq_pred_probs = sq_pred_probs.sort_values("pred_prob", ascending=False)
    new_pred_probs = new_pred_probs.sort_values("pred_prob", ascending=False)
    sq_pred_probs["index"] = sq_pred_probs.index
    new_pred_probs["index"] = new_pred_probs.index
    sq_pred_probs = sq_pred_probs.drop_duplicates(subset="index", keep="first")
    new_pred_probs = new_pred_probs.drop_duplicates(subset="index", keep="first")
    sq_pred_probs = sq_pred_probs.pred_prob
    new_pred_probs = new_pred_probs.pred_prob
    sq_min_prob = round(min(sq_pred_probs), 3)
    new_min_prob = round(min(new_pred_probs), 3)
    sq_avg_prob = round(sum(sq_pred_probs) / len(sq_pred_probs), 3)
    new_avg_prob = round(sum(new_pred_probs) / len(new_pred_probs), 3)
    update_results({
        "100% recall acheived" : full_recall,
        'minimum probability required for status quo model' : sq_min_prob,
        'minimum probability required for new model' : new_min_prob,
        'average probability required for status quo model' : sq_avg_prob,
        'average probability required for new model' : new_avg_prob,
        'false positives with status quo' : sq_false_positives,
        'false positives with new' : new_false_positives,
    })
    if not full_recall:
        logger.warning(
            "100% recall not acheived with new model - archiving it "
            "and maintaining status quo!"
        )
        if test:
            logger.info("skipping files transfers because running in test mode")
        else:
            for artifact in ["model", "features"]:
                os.rename(
                    f"new_rf_{artifact}.pkl",
                    f"model_archive/rf_new_{artifact}-{datetime.datetime.now().date()}.pkl",
                )
    else:
        logger.info("100% recall acheived! Adopting new model and archiving old one.")
        if test:
            logger.info("skipping files transfers because running in test mode")
        else:
            for artifact in ["model", "features"]:
                os.rename(
                    f"rf_{artifact}.pkl",
                    f"model_archive/rf_{artifact}-{datetime.datetime.now().date()}.pkl",
                )
                os.rename(f"new_rf_{artifact}.pkl", f"rf_{artifact}.pkl")
        for metric, new, sq in zip(
            ("false positive(s)", "max threshold", "average prediction probability"),
            (new_false_positives, new_min_prob, new_avg_prob),
            (sq_false_positives, sq_min_prob, sq_avg_prob),
        ):
            if metric == "false positive(s)":
                if new <= sq:
                    good_outcome = True
                else:
                    good_outcome = False
            elif new >= sq:
                good_outcome = True
            else:
                good_outcome = False
            if good_outcome:
                logger.info(
                    f"New model produced {new} {metric}, "
                    f"which is better or equal to status quo of {sq}."
                )
            else:
                logger.warning(
                    f"Might want to investigate new model - new model produced "
                    f"{new} {metric}, compared to status quo of {sq}"
                )


if __name__ == "__main__":
    train_model()
