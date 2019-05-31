import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from imblearn.over_sampling import SMOTE
from sklearn.model_selection import KFold
from sklearn.metrics import f1_score
import pickle
from scraper import scrape
from wrangler import wrangle
from matcher import match
from matcher_build import match_build
from db_tools import create_connection
import sys
import logging


logger = logging.getLogger(__name__)
log_handler = logging.StreamHandler(sys.stdout)
log_handler.setFormatter(
    logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s - %(funcName)s - line %(lineno)d"
    )
)
logger.addHandler(log_handler)
logger.setLevel(logging.INFO)

def save_model(model):
    logger.info("saving random forest classifier")
    with open("./rf_model.pkl", "wb") as output:
        pickle.dump(model, output)

def save_feature_list(columns):
    logger.info("saving list of features for random forest classifier")
    with open("./rf_features.pkl", "wb") as output:
        pickle.dump(columns, output)

def load_model():
    logger.info("loading random forest classifier")
    with open("./rf_model.pkl", "rb") as input_file:
        return pickle.load(input_file)

def build_train_set():
    logger.info("building dataset for training random forest classifier")
    match_query = """
                        SELECT 
                            df_dilfo.job_number,
                            df_dilfo.city,
                            df_dilfo.address,
                            df_dilfo.title,
                            df_dilfo.owner,
                            df_dilfo.contractor,
                            df_dilfo.engineer,
                            df_dilfo.receiver_email,
                            df_dilfo.cc_email,
                            df_dilfo.quality,
                            df_matched.dcn_key,
                            df_matched.ground_truth
                        FROM 
                            df_dilfo 
                        LEFT JOIN 
                            df_matched
                        ON 
                            df_dilfo.job_number=df_matched.job_number
                        WHERE 
                            df_dilfo.closed=1
                        AND 
                            df_matched.ground_truth=1
                    """

    with create_connection() as conn:
        test_df_dilfo = pd.read_sql(match_query, conn)
    test_web_df = scrape(ref=test_df_dilfo)
    test_web_df = wrangle(test_web_df)

    # Get some certificates that are definitely not matches provide some false matches to train from
    start_date = '2011-01-01'
    end_date = '2011-04-30'
    hist_query = "SELECT * FROM df_hist WHERE pub_date BETWEEN ? AND ? ORDER BY pub_date"
    with create_connection() as conn:
        rand_web_df = pd.read_sql(hist_query, conn, params=[start_date, end_date])
    rand_web_df = wrangle(rand_web_df)

    for i, test_row_dilfo in test_df_dilfo.iterrows():
        test_row_dilfo = wrangle(test_row_dilfo.to_frame().transpose())  # .iterows returns a pd.Series for every row so this turns it back into a dataframe to avoid breaking any methods downstream
        rand_web_df = rand_web_df.sample(n=len(test_df_dilfo), random_state=i)
        close_matches = match_build(test_row_dilfo, test_web_df)
        random_matches = match_build(test_row_dilfo, rand_web_df)
        matches = close_matches.append(random_matches)
        matches['ground_truth'] = matches.dcn_key.apply(
            lambda x: 1 if x == test_row_dilfo.dcn_key.iloc[0] else 0)
        matches['dilfo_job_number'] = test_row_dilfo.job_number.iloc[0]
        matches['title_length'] = matches.title.apply(len)
        try:
            all_matches = all_matches.append(matches)
        except NameError:
            all_matches = matches
    all_matches.to_csv(f'./train_set.csv', index=False)

def train_model(prob_thresh=0.65):
    logger.info("training random forest classifier")
    df = pd.read_csv('./train_set.csv')
    X = df[[x for x in df.columns if x.endswith('_score')]]
    save_feature_list(X.columns)
    y = df[['ground_truth']]
    clf = RandomForestClassifier(n_estimators=100, max_depth=5, random_state=42)
    sm = SMOTE(random_state=42, ratio = 1)
    kf = KFold(n_splits=3, shuffle=True, random_state=41)
    rc_cum, pr_cum, f1_cum = [], [], []
    split_no = 0
    for train_index, test_index in kf.split(X):
        split_no +=1
        logger.info(f"K-Split #{split_no}...")
        X_train, X_test = X.values[train_index], X.values[test_index]
        y_train, y_test = y.values[train_index], y.values[test_index]
        X_train_smo, y_train_smo = sm.fit_sample(X_train, y_train)
        clf.fit(X_train_smo, y_train_smo)
        prob = clf.predict_proba(X_test)
        pred = [1 if x >= prob_thresh else 0 for x in clf.predict_proba(X_test)[:,1]]
        y_test = y_test.reshape(y_test.shape[0],) # shitty little workaround required due to pandas -> numpy  conversion
        results = pd.DataFrame({'truth':y_test, 'total_score':X_test[:,-1], 'prob':prob[:,1], 'pred':pred})
        rc = len(results[(results.truth==1)&(results.pred==1)]) / len(results[results.truth==1])
        pr = len(results[(results.truth==1)&(results.pred==1)]) / len(results[results.pred==1])
        f1 = f1_score(y_test, pred)
        show_res = results[(results.truth==1)|(results.pred==1)|(results.total_score>0.6)|(results.prob>0.3)].sort_values(['total_score'], ascending=False)
        logger.debug("\nshow_res\n")
        logger.info(f'number of truthes to learn from: {len([x for x in y_train if x==1])} out of {len(y_train)}')
        logger.info(f'number of tests: {len(results[results.truth==1])}')
        feat_imp = pd.DataFrame({'feat':X.columns, 'imp':clf.feature_importances_}).sort_values('imp', ascending=False)
        logger.debug(f"\nfeat_imp\n")
        logger.info(f'top feature is `{feat_imp.iloc[0].feat}` with factor of {round(feat_imp.iloc[0].imp, 3)}')
        logger.info(f'recall: {round(rc, 3)}')
        logger.info(f'precision: {round(pr, 3)}')
        logger.info(f'f1 score: {round(f1, 3)}')
        rc_cum.append(rc)
        pr_cum.append(pr)
        f1_cum.append(f1)
    logger.info(f'average recall: {round(sum(rc_cum)/len(rc_cum), 3)}')
    logger.info(f'average precision: {round(sum(pr_cum)/len(pr_cum), 3)}')
    logger.info(f'avergae f1 score: {round(sum(f1_cum)/len(f1_cum), 3)}')
    X_smo, y_smo = sm.fit_sample(X, y)
    clf.fit(X_smo, y_smo)
    save_model(clf)
    return rc_cum, pr_cum, f1_cum

if __name__ == '__main__':
    train_model()
