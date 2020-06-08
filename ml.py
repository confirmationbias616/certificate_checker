import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
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
from statistics import mean


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
    use_smote=load_config()["machine_learning"]["use_smote"]
):
    """Trains instance of scikit-learn's RandomForestClassifier model on the training dataset
    from project's root directory (typically produced by function ml.build_train_set) and saves
    trained model to root directory as well.
    
    Parameters:
     - `prob_thresh` (float): probability threshold which the classifier will use to determine
     whether or not there is a match. Scikit-learn's default threshold is 0.5 but this is being
     disregarded. Note that this threshold doesn't impact the actual training of the model - 
     only its custom predictions and performance metrics. Default loads from config file.
     - `use_smote` (boolean): whether or not the SMOTE algorithm should be applied to the labeled
     data before training the model. Default loads from config file.

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
    clf = RandomForestClassifier(n_estimators=100, random_state=42)
    sm = SMOTE(random_state=42, ratio=1)
    kf = KFold(n_splits=3, shuffle=True, random_state=41)
    rc_cum, pr_cum, f1_cum = [], [], []
    split_no = 0
    for train_index, test_index in kf.split(X):
        split_no += 1
        logger.info(f"K-Split #{split_no}...")
        X_train, X_test = X.values[train_index], X.values[test_index]
        y_train, y_test = y.values[train_index], y.values[test_index]
        if use_smote:
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
        logger.debug(
            f"number of truthes to learn from: {len([x for x in y_train if x==1])} out of {len(y_train)}"
        )
        logger.debug(f"number of tests: {len(results[results.truth==1])}")
        logger.debug(f"recall: {round(rc, 3)}")
        logger.debug(f"precision: {round(pr, 3)}")
        logger.debug(f"f1 score: {round(f1, 3)}")
        rc_cum.append(rc)
        pr_cum.append(pr)
        f1_cum.append(f1)
    logger.debug(f"average recall: {round(sum(rc_cum)/len(rc_cum), 3)}")
    logger.debug(f"average precision: {round(sum(pr_cum)/len(pr_cum), 3)}")
    logger.debug(f"avergae f1 score: {round(sum(f1_cum)/len(f1_cum), 3)}")
    if use_smote:
        X_final, y_final = sm.fit_sample(X, y)
    else:
        X_final, y_final = X, y 
    clf.fit(X_final, y_final)
    feat_imp = pd.DataFrame(
        {"feat": X.columns, "imp": clf.feature_importances_}
    ).sort_values("imp", ascending=False)
    logger.info("top features are:")
    for _, row in feat_imp.iterrows():
        logger.info(
            "\t" + "{:<25}".format(row['feat']) + "\t" + str(round(row['imp']*100, 1)) + "\t"
        )
    save_model(clf)
    return rc_cum, pr_cum, f1_cum


def validate_model(
    prob_thresh=load_config()["machine_learning"]["prboability_thresholds"]["general"],
    test=False
):
    """Compares new model with status quo production model and compiles/reports the results.
    Based on results, will either replace model and archive old one or just maintain status quo.
    
    Parameters:
     - `prob_thresh` (float): probability threshold which the classifier will use to determine
     whether or not there is a match.
     - `test` (bool): whether in testing or not, will dtermine flow of operations and mute emails appropriately.

    """
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
            web_certificates.cert_id,
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
        prob_thresh=prob_thresh,
    )
    analysis_df = pd.merge(
        new_results[['job_number', 'cert_id', 'pred_prob', 'pred_match', 'total_score']],
        validate_company_projects[['job_number', 'cert_id', 'ground_truth']],
        how='left',
        on=['job_number', 'cert_id']
    )
    analysis_df['ground_truth'] = analysis_df.ground_truth.apply(lambda x: 1 if x == 1.0 else 0)
    tp = len(analysis_df[(analysis_df.pred_match == 1) & (analysis_df.ground_truth == 1)])
    fp = len(analysis_df[(analysis_df.pred_match == 1) & (analysis_df.ground_truth == 0)])
    tn = len(analysis_df[(analysis_df.pred_match == 0) & (analysis_df.ground_truth == 0)])
    fn = len(analysis_df[(analysis_df.pred_match == 0) & (analysis_df.ground_truth == 1)])
    if fn:
        logger.warning(f"match for project #{list(analysis_df[(analysis_df.pred_match == 0) & (analysis_df.ground_truth == 1)]['job_number'])} was not detected.")
    logger.info(f"true postives: {tp}")
    logger.info(f"false postives: {fp}")
    logger.info(f"true negatives: {tn}")
    logger.info(f"false negatives: {fn}")
    recall = tp / (tp + fn)
    precision = tp / (tp + fp)
    logger.info(f"recall: {recall}")
    logger.info(f"precision: {precision}")
    min_prob = min(analysis_df[analysis_df.ground_truth == 1.0]['pred_prob'])
    logger.info(f"minimum probability threshhold to acheive 100% recall: {min_prob}")
    analysis_df['adj_pred_match'] = analysis_df.pred_prob.apply(lambda x: x >= min_prob)
    avg_prob = mean(analysis_df[analysis_df.ground_truth == 1.0]['pred_prob'])
    logger.debug(analysis_df[analysis_df.adj_pred_match])
    signal_and_noise = analysis_df[analysis_df.pred_prob > -0.1]
    signal = signal_and_noise[signal_and_noise.ground_truth == 1.0]['pred_prob']
    noise = signal_and_noise[signal_and_noise.ground_truth != 1.0]['pred_prob']
    interval = 0.1
    bottom_ranges = np.arange(0, 1, interval)
    ground_truths, false_matches = [], []
    for bottom_range in bottom_ranges:
        bottom_range = round(bottom_range, 1)
        upper_range = round((bottom_range + interval), 1)
        if bottom_range == 0.0:  # capture all the false matches scored at exactly 0
            bottom_range = -0.1
        ground_truths.append(len([value for value in signal if value <= upper_range and value > bottom_range]))
        false_matches.append(len([value for value in noise if value <= upper_range and value > bottom_range]))
    df = pd.DataFrame({
        'probability score' : bottom_ranges,
        'true match' : ground_truths,
        'false match' : false_matches
    })
    p1 = plt.bar(df['probability score'], df['true match'], width=0.07, align='edge', color=(112/255, 94/255, 204/255, 1))
    p2 = plt.bar(df['probability score'], df['false match'], width=0.07, align='edge', bottom=df['true match'], color=(112/255, 94/255, 134/255, 1))
    t = plt.axvline(x=prob_thresh, color=(70/255, 70/255, 80/255, 1), linestyle='--')
    plt.ylabel('# of matches')
    plt.xlabel('predicted probability of match')
    ax = plt.axes()
    ax.xaxis.set_major_locator(MaxNLocator(integer=True))
    # ax.set_yscale('log', nonposy='clip')  # too glitchy to use
    plt.xticks([0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1])
    plt.title('Precision Spread on Validation Data\n')
    plt.legend((p1[0], p2[0]), ('true match', 'false match'))
    # ax = plt.axes()
    # for spine in ax.spines:
    #     ax.spines[spine].set_visible(False)
    legend = plt.legend((p1[0], p2[0], t), ('true match', 'false match', 'decision threshold'), frameon=1)
    frame = legend.get_frame()
    frame.set_alpha(0)
    if not test:  # will also display inside jupyter notebook regardless (if %matplotlib inline)
        plt.savefig('static/precision_spread.png', transparent=True, dpi=300)
    if recall < 1.0:
        adj_tp = len(analysis_df[(analysis_df.adj_pred_match == 1) & (analysis_df.ground_truth == 1)])
        adj_fp = len(analysis_df[(analysis_df.adj_pred_match == 1) & (analysis_df.ground_truth == 0)])
        adj_tn = len(analysis_df[(analysis_df.adj_pred_match == 0) & (analysis_df.ground_truth == 0)])
        adj_fn = len(analysis_df[(analysis_df.adj_pred_match == 0) & (analysis_df.ground_truth == 1)])
        logger.info(f"adjusted true postives: {adj_tp}")
        logger.info(f"adjusted false postives: {adj_fp}")
        logger.info(f"adjusted true negatives: {adj_tn}")
        logger.info(f"adjusted false negatives: {adj_fn}")
        adj_recall = adj_tp / (adj_tp + adj_fn)
        adj_precision = adj_tp / (adj_tp + adj_fp)
        logger.info(f"adjusted recall: {adj_recall}")
        logger.info(f"adjusted precision: {adj_precision}")
        logger.info(f"Would have had {adj_fp} false positives ({adj_precision}% precision) if threshold was adjusted down to acheive 100%")
    try:
        sq_results = match(
            version="status_quo",
            company_projects=validate_company_projects,
            df_web=validate_web_df,
            test=True,
            prob_thresh=prob_thresh,
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
                prob_thresh=prob_thresh,
            )
    sq_analysis_df = pd.merge(
        sq_results[['job_number', 'cert_id', 'pred_prob', 'pred_match', 'total_score']],
        validate_company_projects[['job_number', 'cert_id', 'ground_truth']],
        how='left',
        on=['job_number', 'cert_id']
    )
    sq_analysis_df['ground_truth'] = sq_analysis_df.ground_truth.apply(lambda x: 1 if x == 1.0 else 0)
    sq_tp = len(sq_analysis_df[(sq_analysis_df.pred_match == 1) & (sq_analysis_df.ground_truth == 1)])
    sq_fp = len(sq_analysis_df[(sq_analysis_df.pred_match == 1) & (sq_analysis_df.ground_truth == 0)])
    sq_tn = len(sq_analysis_df[(sq_analysis_df.pred_match == 0) & (sq_analysis_df.ground_truth == 0)])
    sq_fn = len(sq_analysis_df[(sq_analysis_df.pred_match == 0) & (sq_analysis_df.ground_truth == 1)])
    if sq_fn:
        logger.warning(f"match for project #{list(sq_analysis_df[(sq_analysis_df.pred_match == 0) & (sq_analysis_df.ground_truth == 1)]['job_number'])} was not detected.")
    logger.info(f"true postives: {sq_tp}")
    logger.info(f"false postives: {sq_fp}")
    logger.info(f"true negatives: {sq_tn}")
    logger.info(f"false negatives: {sq_fn}")
    sq_recall = sq_tp / (sq_tp + sq_fn)
    sq_precision = sq_tp / (sq_tp + sq_fp)
    logger.info(f"recall: {sq_recall}")
    logger.info(f"precision: {sq_precision}")
    sq_min_prob = min(sq_analysis_df[sq_analysis_df.ground_truth == 1.0]['pred_prob'])
    logger.info(f"minimum probability threshhold to acheive 100% recall: {sq_min_prob}")
    sq_analysis_df['adj_pred_match'] = sq_analysis_df.pred_prob.apply(lambda x: x >= sq_min_prob)
    sq_avg_prob = mean(sq_analysis_df[sq_analysis_df.ground_truth == 1.0]['pred_prob'])
    logger.debug(sq_analysis_df[sq_analysis_df.adj_pred_match])
    update_results({
        "probability threshold": prob_thresh,
        "SMOTE": load_config()["machine_learning"]["use_smote"],
        "100% recall acheived" : True if int(recall) == 1 else False,
        'minimum probability required for status quo model' : sq_min_prob,
        'minimum probability required for new model' : min_prob,
        'average probability required for status quo model' : sq_avg_prob,
        'average probability required for new model' : avg_prob,
        'false positives with status quo' : sq_fp,
        'false positives with new' : fp,
        'precision': precision,
    })
    if recall < 1.0:
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
            (fp, min_prob, avg_prob),
            (sq_fp, sq_min_prob, sq_avg_prob),
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
    build_train_set()
    validate_model()
