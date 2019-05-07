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

def save_model(model):
    with open("./rf_model.pkl", "wb") as output:
        pickle.dump(model, output)

def save_feature_list(columns):
    with open("./rf_features.pkl", "wb") as output:
        pickle.dump(columns, output)

def load_model():
    with open("./rf_model.pkl", "rb") as input_file:
        return pickle.load(input_file)

def build_train_set():
    match_query = "SELECT * FROM dilfo_matched"
    with create_connection() as conn:
        test_df_dilfo = pd.read_sql(match_query, conn)
    test_web_df = scrape(ref=test_df_dilfo)
    test_web_df = wrangle(test_web_df)
    
    # Get some certificates that are definitely not matches provide some false matches to train from
    start_date = '2011-01-01'
    end_date = '2011-04-30'
    hist_query = "SELECT * FROM hist_certs WHERE pub_date BETWEEN ? AND ? ORDER BY pub_date"
    with create_connection() as conn:
        rand_web_df = pd.read_sql(hist_query, conn, params=[start_date, end_date])
    rand_web_df = wrangle(rand_web_df)
    
    for i, test_row_dilfo in test_df_dilfo.iterrows():
        test_row_dilfo = wrangle(test_row_dilfo.to_frame().transpose())  # .iterows returns a pd.Series for every row so this turns it back into a dataframe to avoid breaking any methods downstream
        rand_web_df = rand_web_df.sample(n=len(test_df_dilfo), random_state=i)
        close_matches = match_build(test_row_dilfo, test_web_df)
        random_matches = match_build(test_row_dilfo, rand_web_df)
        matches = close_matches.append(random_matches)
        matches['ground_truth'] = matches.cert_url.apply(
            lambda x: 1 if x == test_row_dilfo.link_to_cert.iloc[0] else 0)
        matches['dilfo_job_number'] = test_row_dilfo.job_number.iloc[0]
        matches['title_length'] = matches.title.apply(len)
        try:
            all_matches = all_matches.append(matches)
        except NameError:
            all_matches = matches

    all_matches.to_csv(f'./train_set.csv', index=False)

def train_model():
    df = pd.read_csv('./train_set.csv')
    X = df[[x for x in df.columns if x.endswith('_score')]]
    save_feature_list(X.columns)
    y = df[['ground_truth']]
    clf = RandomForestClassifier(n_estimators=100, max_depth=5, random_state=42)
    sm = SMOTE(random_state=42, ratio = 1)
    kf = KFold(n_splits=3, shuffle=True, random_state=41)
    rc_cum, pr_cum, f1_cum = [], [], []
    for train_index, test_index in kf.split(X):
        X_train, X_test = X.values[train_index], X.values[test_index]
        y_train, y_test = y.values[train_index], y.values[test_index]
        X_train_smo, y_train_smo = sm.fit_sample(X_train, y_train)
        clf.fit(X_train_smo, y_train_smo)
        pred = clf.predict(X_test)
        prob = clf.predict_proba(X_test)
        y_test = y_test.reshape(y_test.shape[0],) # shitty little workaround required due to pandas -> numpy  conversion
        results = pd.DataFrame({'truth':y_test, 'total_score':X_test[:,-1], 'prob':prob[:,1], 'pred':pred})
        rc = len(results[(results.truth==1)&(results.pred==1)]) / len(results[results.truth==1])
        pr = len(results[(results.truth==1)&(results.pred==1)]) / len(results[results.pred==1])
        f1 = f1_score(y_test, pred)
        print(results[(results.truth==1)|(results.pred==1)|(results.total_score>0.6)|(results.prob>0.3)].sort_values(['total_score'], ascending=False))
        print(f'number of truthes to learn from: {len([x for x in y_train if x==1])} out of {len(y_train)}')
        print(f'number of tests: {len(results[results.truth==1])}')
        print(pd.DataFrame({'feat':X.columns, 'imp':clf.feature_importances_}).sort_values('imp', ascending=False))
        print(f'recall: {rc}')
        print(f'precision: {pr}')
        print(f'f1 score: {f1}\n\n')
        rc_cum.append(rc)
        pr_cum.append(pr)
        f1_cum.append(f1)
    print(f'average recall: {sum(rc_cum)/len(rc_cum)}')
    print(f'average precision: {sum(pr_cum)/len(pr_cum)}')
    print(f'avergae f1 score: {sum(f1_cum)/len(f1_cum)}')
    X_smo, y_smo = sm.fit_sample(X, y)
    clf.fit(X_smo, y_smo)
    save_model(clf)
    return rc_cum, pr_cum, f1_cum

def evaluate(sample):
    clf = load_model()
    if type(sample) == str:
        match = clf.predict(np.array(sample).reshape(1, -1))
        prob = clf.predict_proba(np.array(sample).reshape(1, -1))[0]
        string = f"{'yes' if match else 'no'} by a probability of " \
            f"{int(prob[1]*100) if match else int(prob[0]*100)}%"
        return match, prob, string
    elif type(sample) == pd.DataFrame and len(sample) > 1:
        X = sample[[x for x in sample.columns if x.endswith('_score')]]
        pred = clf.predict(X)
        prob = clf.predict_proba(X)
        results = pd.DataFrame({'total_score':X.total_score, 'prob':prob[:,1], 'pred':pred})
        return results
    else:
        raise TypeError (f"evaluate did not receive propper input. Type received: {type(sample)}")

if __name__ == '__main__':
    train_model()
